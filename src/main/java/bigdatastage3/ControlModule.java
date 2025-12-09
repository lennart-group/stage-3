package bigdatastage3;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Orchestriert: Ingestion -> (warten) -> Indexing.
 * Hält Buch über bereits verarbeitete book_ids in indexed_books.txt.
 */
public class ControlModule {

  private final HttpClient http = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();
  private final Gson gson = new Gson();

  private String ingestionBase = "http://localhost:7000"; // z.B. http://localhost:7001
  private String indexingBase = "http://localhost:7004"; // z.B. http://localhost:7002
  private String searchBase = "http://localhost:7003"; // optional, z.B. http://localhost:7003

  private final Path processedListFile; // z.B. control/indexed_books.txt
  private final Set<Integer> processedCache = ConcurrentHashMap.newKeySet();

  public ControlModule(String ingestionBase, String indexingBase, String searchBase, Path processedListFile) {
    this.ingestionBase = stripTrailingSlash(ingestionBase);
    this.indexingBase = stripTrailingSlash(indexingBase);
    this.searchBase = searchBase == null ? null : stripTrailingSlash(searchBase);
    this.processedListFile = processedListFile;
    try {
      loadProcessedCache();
    } catch (IOException ignored) {
    }
  }

  private static String stripTrailingSlash(String s) {
    return s != null && s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
  }

  private void loadProcessedCache() throws IOException {
    if (!Files.exists(processedListFile))
      return;
    try (var lines = Files.lines(processedListFile)) {
      lines.map(String::trim)
          .filter(x -> !x.isEmpty() && x.chars().allMatch(Character::isDigit))
          .map(Integer::parseInt)
          .forEach(processedCache::add);
    }
  }

  public boolean alreadyProcessed(int bookId) {
    return processedCache.contains(bookId);
  }

  private synchronized void markProcessed(int bookId) throws IOException {
    if (processedCache.contains(bookId))
      return;
    Files.createDirectories(Optional.ofNullable(processedListFile.getParent()).orElse(Path.of(".")));
    Files.writeString(processedListFile, bookId + System.lineSeparator(),
        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    processedCache.add(bookId);
  }

  public Map<String, Object> ingestAndIndex(int bookId) throws Exception {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("book_id", bookId);

    if (alreadyProcessed(bookId)) {
      out.put("status", "skipped");
      out.put("reason", "already processed");
      return out;
    }

    // 1) Ingestion triggern
    HttpResponse<String> ingestResp = sendPost(ingestionBase + "/ingest/" + bookId, null, 30);
    out.put("ingest_trigger_code", ingestResp.statusCode());

    // 2) Ingestion-Status pollen bis "available"
    boolean available = poll(() -> isIngestionAvailable(bookId),
        Duration.ofSeconds(envInt("CONTROL_INGEST_MAX_WAIT_SEC", 120)),
        Duration.ofMillis(envInt("CONTROL_INGEST_POLL_MS", 1500)));
    if (!available) {
      out.put("status", "failed");
      out.put("stage", "ingestion_wait_timeout");
      return out;
    }

    // 3) Indexing triggern
    HttpResponse<String> indexResp = sendPost(indexingBase + "/index/update/" + bookId, null, 60);
    out.put("index_trigger_code", indexResp.statusCode());

    // (optional) kurz auf Index-Fortschritt schauen, wenn /index/status existiert
    try {
      HttpResponse<String> st = sendGet(indexingBase + "/index/status", 10);
      out.put("index_status_snapshot", safeJsonToMap(st.body()));
    } catch (Exception ignored) {
    }

    // 4) (optional) Search-Caches refreshen, falls ihr so einen Endpoint habt
    try {
      if (searchBase != null) {
        sendPost(searchBase + "/search/refresh", null, 10);
        out.put("search_refreshed", true);
      }
    } catch (Exception e) {
      out.put("search_refreshed", false);
      out.put("search_refresh_error", e.getMessage());
    }

    markProcessed(bookId);
    out.put("status", "ok");
    return out;
  }

  /** Batch-Verarbeitung: bricht nicht beim ersten Fehler ab. */
  public List<Map<String, Object>> ingestAndIndexBatch(Collection<Integer> ids) {
    List<Map<String, Object>> results = new ArrayList<>();
    for (int id : ids) {
      try {
        results.add(ingestAndIndex(id));
      } catch (Exception e) {
        results.add(Map.of(
            "book_id", id,
            "status", "error",
            "message", e.getMessage()));
      }
    }
    return results;
  }

  public String downloadBook(int bookId) throws Exception {
    HttpResponse<String> statusResp = sendPost(ingestionBase + "/ingest/status/" + bookId, null, 30);
    JsonObject json = JsonParser.parseString(statusResp.body()).getAsJsonObject();
    String status = json.get("status").getAsString();
    if (status == "available") {
      return "Book is already in the database.";
    }
    HttpResponse<String> ingestResp = sendPost(ingestionBase + "/ingest/" + bookId, null, 30);
    if (ingestResp.statusCode() != 200) {
      return "Ingestion failed with status code: " + ingestResp.statusCode();
    }
    return "Ingestion completed for book ID: " + bookId;
  }

  public String indexBook(int bookId) throws Exception {
    boolean indexed = Files.lines(processedListFile).anyMatch(line -> line.equals(Integer.toString(bookId)));
    if (indexed) {
      return "Book " + bookId + " is already indexed.";
    }
    HttpResponse<String> indexResp = sendPost(indexingBase + "/index/update/" + bookId, null, 60);
    if (indexResp.statusCode() != 200) {
      return "Indexing failed with status code: " + indexResp.statusCode();
    }
    return "Indexing completed for book ID: " + bookId;
  }

  // TODO: Search function

  // ---------- helpers ----------

  private boolean isIngestionAvailable(int bookId) throws Exception {
    HttpResponse<String> resp = sendGet(ingestionBase + "/ingest/status/" + bookId, 8);
    if (resp.statusCode() != 200 || resp.body() == null)
      return false;
    Map<String, Object> m = safeJsonToMap(resp.body());
    Object s = m.get("status");
    if (s == null)
      s = m.get("state");
    String state = s == null ? "" : s.toString().toLowerCase(Locale.ROOT);
    // akzeptiere "available", "ready", "downloaded"
    return state.contains("avail") || state.contains("ready") || state.contains("download");
  }

  private Map<String, Object> safeJsonToMap(String json) {
    try {
      Type t = new TypeToken<Map<String, Object>>() {
      }.getType();
      Map<String, Object> m = gson.fromJson(json, t);
      return m == null ? Map.of() : m;
    } catch (Exception e) {
      return Map.of("raw", truncate(json, 500));
    }
  }

  private static String truncate(String s, int max) {
    if (s == null)
      return null;
    return s.length() <= max ? s : s.substring(0, max) + "...";
  }

  private boolean poll(CheckedSupplier<Boolean> cond, Duration maxWait, Duration interval) throws Exception {
    long deadline = System.nanoTime() + maxWait.toNanos();
    while (System.nanoTime() < deadline) {
      if (cond.get())
        return true;
      Thread.sleep(interval.toMillis());
    }
    return false;
  }

  private HttpResponse<String> sendPost(String url, String body, int timeoutSec)
      throws IOException, InterruptedException {
    HttpRequest.Builder b = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(timeoutSec))
        .POST(body == null ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
    return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> sendGet(String url, int timeoutSec) throws IOException, InterruptedException {
    HttpRequest req = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(timeoutSec))
        .GET().build();
    return http.send(req, HttpResponse.BodyHandlers.ofString());
  }

  @FunctionalInterface
  private interface CheckedSupplier<T> {
    T get() throws Exception;
  }

  private static int envInt(String key, int def) {
    try {
      return Integer.parseInt(System.getenv().getOrDefault(key, String.valueOf(def)));
    } catch (Exception e) {
      return def;
    }
  }
}
