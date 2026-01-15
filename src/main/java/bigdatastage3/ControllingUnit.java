package bigdatastage3;

import com.google.gson.Gson;
import io.javalin.Javalin;
import io.javalin.http.Context;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

public class ControllingUnit {

  // ---------- configuration / state ----------
  private static final Gson gson = new Gson();
  private static Path CONTROL_DIR;
  private static Path PROCESSED_FILE;

  private static String INGEST_API;
  private static String INDEX_API;
  private static String SEARCH_API;

  private static final HttpClient httpClient = HttpClient.newHttpClient();

  public static void main(String[] args) throws IOException {

    INGEST_API = System.getenv("INGEST_API");
    INDEX_API = System.getenv("INDEX_API");
    SEARCH_API = System.getenv("SEARCH_API");
    CONTROL_DIR = Paths.get("control");
    int PORT = Integer.parseInt(System.getenv("CONTROLLER_PORT"));
    PROCESSED_FILE = CONTROL_DIR.resolve("processed_books.txt");

    ensureControlDir();

    Javalin app = Javalin.create().start(PORT);

app.before(ctx -> {
    ctx.header("Access-Control-Allow-Origin", "*");
    ctx.header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
    ctx.header("Access-Control-Allow-Headers", "Content-Type,Authorization");
});

app.options("/*", ctx -> ctx.status(204));

    // ---------- endpoints ----------
    app.get("/status", ControllingUnit::status);
    app.post("/control/run/{book_id}", ControllingUnit::processBook);
    app.post("/control/run-batch", ControllingUnit::processBooksBatch);
    app.get("/control/processed", ControllingUnit::processedBooks);
    app.get("/search", ControllingUnit::searchBooks);

    System.out.println("🚀 ControllingUnit API running on port: " + PORT);
  }

  // ---------- handlers ----------

  private static void status(Context ctx) {
    Map<String, Object> status = new LinkedHashMap<>();
    status.put("service", "controller");
    status.put("processed_file", PROCESSED_FILE.toString());
    ctx.result(gson.toJson(status));
  }

  private static void processBook(Context ctx) {
    String idStr = ctx.pathParam("book_id");
    try {
      int bookId = Integer.parseInt(idStr);
      if (alreadyProcessed(bookId)) {
        ctx.result(gson.toJson(Map.of(
            "book_id", bookId,
            "status", "already processed")));
        return;
      }

      // Download book
      String ingestResult = callApi(INGEST_API + "/ingest/" + bookId);
      // Index book
      String indexResult = callApi(INDEX_API + "/index/update/" + bookId);

      markProcessed(bookId);

      ctx.result(gson.toJson(Map.of(
          "book_id", bookId,
          "ingest", ingestResult,
          "index", indexResult,
          "status", "success")));

    } catch (NumberFormatException e) {
      ctx.status(400).json(Map.of("error", "Book ID must be a number"));
    } catch (Exception e) {
      ctx.status(500).json(Map.of("error", e.getMessage()));
    }
  }

  private static void processBooksBatch(Context ctx) {
    Map<String, Object> body = ctx.bodyAsClass(Map.class);
    List<Integer> bookIds = ((List<?>) body.getOrDefault("book_ids", List.of()))
        .stream()
        .map(Object::toString)
        .map(Integer::parseInt)
        .collect(Collectors.toList());

    List<Map<String, Object>> results = new ArrayList<>();
    for (int id : bookIds) {
      try {
        String ingestResult = callApi(INGEST_API + "/ingest/" + id);
        String indexResult = callApi(INDEX_API + "/index/update/" + id);
        markProcessed(id);

        results.add(Map.of(
            "book_id", id,
            "ingest", ingestResult,
            "index", indexResult,
            "status", "success"));

      } catch (Exception e) {
        results.add(Map.of(
            "book_id", id,
            "status", "error",
            "message", e.getMessage()));
      }
    }

    ctx.json(results);
  }

  private static void processedBooks(Context ctx) {
    try {
      if (Files.exists(PROCESSED_FILE)) {
        List<String> lines = Files.readAllLines(PROCESSED_FILE, StandardCharsets.UTF_8);
        ctx.json(lines);
      } else {
        ctx.json(Collections.emptyList());
      }
    } catch (IOException e) {
      ctx.status(500).json(Map.of("error", e.getMessage()));
    }
  }

  private static void searchBooks(Context ctx) {
    String query = ctx.queryParam("q");
    String author = ctx.queryParam("author");
    String language = ctx.queryParam("language");
    String year = ctx.queryParam("year");

    if (query == null || query.trim().isEmpty()) {
      ctx.status(400).json(Map.of("error", "Query parameter 'q' is required."));
      return;
    }

    try {
      String url = SEARCH_API + "/search?q=" + query;
      if (author != null)
        url += "&author=" + author;
      if (language != null)
        url += "&language=" + language;
      if (year != null)
        url += "&year=" + year;

      String response = callApi(url);
      ctx.result(response);
    } catch (Exception e) {
      ctx.status(500).json(Map.of("error", "Search failed: " + e));
    }
  }

  // ---------- helpers ----------

  private static String callApi(String url) throws IOException, InterruptedException {
    HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    return response.body();
  }

  private static void ensureControlDir() throws IOException {
    if (!Files.exists(CONTROL_DIR))
      Files.createDirectories(CONTROL_DIR);
    if (!Files.exists(PROCESSED_FILE))
      Files.createFile(PROCESSED_FILE);
  }

  private static void markProcessed(int bookId) throws IOException {
    try (BufferedWriter bw = Files.newBufferedWriter(
        PROCESSED_FILE, StandardCharsets.UTF_8,
        StandardOpenOption.APPEND)) {
      bw.write(String.valueOf(bookId));
      bw.newLine();
    }
  }

  private static boolean alreadyProcessed(int bookId) {
    try {
      if (!Files.exists(PROCESSED_FILE))
        return false;
      return Files.readAllLines(PROCESSED_FILE, StandardCharsets.UTF_8)
          .stream()
          .anyMatch(line -> line.trim().equals(String.valueOf(bookId)));
    } catch (IOException e) {
      return false;
    }
  }
}
