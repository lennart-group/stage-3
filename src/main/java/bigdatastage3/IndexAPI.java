package bigdatastage3;

import com.google.gson.Gson;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import io.github.cdimascio.dotenv.Dotenv;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.bson.Document;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import jakarta.jms.JMSException;

public class IndexAPI {

    // ---------- configuration / state ----------
    private static final Gson gson = new Gson();

    private static final Path CONTROL_DIR = Paths.get("control");
    private static final Path INDEXED_FILE = CONTROL_DIR.resolve("indexed_books.txt");

    private static MongoCollection<Document> booksCollection;
    private static MongoDatabase indexDb;
    static LocalDateTime lastUpdate = null;   // exposed for IndexingWorker
    private static MessageBroker broker;

    public static void main(String[] args) throws JMSException {

        Dotenv dotenv = Dotenv.load();
        int PORT = Integer.parseInt(dotenv.get("INDEX_PORT"));

        try {
            MongoDatabase[] dbs = RepositoryConnection.connectToDB();

            ensureControlDir();

            booksCollection = dbs[0].getCollection("books");
            indexDb = dbs[1];
            System.out.println("✅ IndexApi DB initialized");

            // Initialize message broker and start asynchronous worker.
            broker = new MessageBroker();
            IndexingWorker worker = new IndexingWorker(broker, booksCollection);
            broker.subscribe(MessageBroker.QUEUE_DOC_INGESTED, worker);
            System.out.println("📨 IndexingWorker subscribed to " + MessageBroker.QUEUE_DOC_INGESTED);

        } catch (Exception e) {
            System.err.println("An error occured while connecting to the database: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        // Javalin app setup
        Javalin app = Javalin.create(cfg -> cfg.http.defaultContentType = "application/json")
                .start(PORT);

        // endpoints
        app.get("/status", IndexAPI::status);
        app.post("/index/update/{book_id}", IndexAPI::indexSingle);
        app.post("/index/all", IndexAPI::indexAll);
        app.get("/index/status", IndexAPI::indexStatus);

        System.out.println("🚀 Index API running on port: " + PORT);
    }

    // ---------- handlers ----------

    private static void status(Context ctx) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("service", "index-service");
        m.put("control_file", INDEXED_FILE.toString());
        m.put("database", "connected");
        ctx.result(gson.toJson(m));
    }

    private static void indexSingle(Context ctx) {
        String idStr = ctx.pathParam("book_id");
        try {
            int id = Integer.parseInt(idStr);
            if (alreadyIndexed(id)) {
                System.out.printf("Book %d already indexed, returning.\n", id);
                ctx.result(gson.toJson(Map.of(
                        "book_id", id,
                        "index", "already up to date")));
                return;
            }

            String text = booksCollection.find(Filters.eq("id", id))
                    .first()
                    .getString("content");

            if (text == null) {
                ctx.status(404).result(gson.toJson(Map.of(
                        "error", "Book not found: " + id)));
                return;
            }

            // Synchronous indexing triggered by REST call.
            processBook(id, text);

            // Emit "document.indexed" event for observers (optional).
            try {
                broker.sendDocumentIndexed(id);
            } catch (Exception ignored) {
            }

            ctx.result(gson.toJson(Map.of(
                    "book_id", id,
                    "index", "updated")));
        } catch (NumberFormatException nfe) {
            ctx.status(400).result("Invalid book_id: must be a number");
        } catch (Exception e) {
            e.printStackTrace();
            ctx.status(500).result(gson.toJson(Map.of("error", e.getMessage())));
        }
    }

    private static void indexAll(Context ctx) {
        int count = 0;
        int termsTotal = 0;
        try {
            try (MongoCursor<Document> cursor = booksCollection.find().iterator()) {
                while (cursor.hasNext()) {
                    Document d = cursor.next();
                    Integer id = d.getInteger("id");
                    if (id == null)
                        continue;
                    if (alreadyIndexed(id)) {
                        System.out.printf("Book %d already indexed, skipping.\n", id);
                        continue;
                    }

                    String text = d.getString("content");
                    if (text == null) {
                        continue;
                    }

                    processBook(id, text);
                    count++;
                }
            }

            ctx.result(gson.toJson(Map.of(
                    "books_processed", count,
                    "elapsed_time", termsTotal)));
        } catch (Exception e) {
            e.printStackTrace();
            ctx.status(500).result(gson.toJson(Map.of("error", e.getMessage())));
        }
    }

    private static void indexStatus(Context ctx) {
        Map<String, Object> m = new LinkedHashMap<>();
        try {
            m.put("books_indexed", countIndexedFromFile());
            m.put("last_update", lastUpdate != null ? lastUpdate.toString() : "unknown");
            Document stats = indexDb.runCommand(new Document("dbStats", 1));
            double sizeInMB = stats.getLong("dataSize") / (1024 * 1024);
            m.put("index_size_MB", sizeInMB);
            ctx.result(gson.toJson(m));
        } catch (Exception e) {
            ctx.status(500).result(gson.toJson(Map.of("error", e.getMessage())));
        }
    }

    // ---------- core indexing ----------

    /**
     * Tokenizes the text, updates the MongoDB-based inverted index
     * and writes the book ID to the local "indexed_books.txt" control file.
     */
    public static void processBook(int bookId, String text) throws Exception {
        Set<String> terms = tokenize(text);

        for (String t : terms) {
            updateMongoInvertedIndex(t, bookId);
        }

        markIndexed(bookId);
        lastUpdate = LocalDateTime.now();
        System.out.printf("✅ Indexed book %d (%d unique terms).%n", bookId, terms.size());
    }

    private static Set<String> tokenize(String text) {
        Set<String> tokens = new HashSet<>();
        if (text == null)
            return tokens;
        Matcher m = Pattern.compile("\\b[a-z]{2,}\\b").matcher(text.toLowerCase());
        while (m.find())
            tokens.add(m.group());
        return tokens;
    }

    private static void updateMongoInvertedIndex(String term, int bookId) {
        String bucket = term.substring(0, 1);
        MongoCollection<Document> col = indexDb.getCollection(bucket);
        col.updateOne(
                Filters.eq("term", term),
                Updates.addToSet("postings", bookId),
                new UpdateOptions().upsert(true));
    }

    // ---------- IO helpers ----------

    private static void ensureControlDir() throws IOException {
        if (!Files.exists(CONTROL_DIR))
            Files.createDirectories(CONTROL_DIR);
    }

    private static void markIndexed(int bookId) throws IOException {
        ensureControlDir();
        try (BufferedWriter bw = Files.newBufferedWriter(
                INDEXED_FILE, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            bw.write(String.valueOf(bookId));
            bw.newLine();
        }
    }

    private static boolean alreadyIndexed(int bookId) {
        if (!Files.exists(INDEXED_FILE))
            return false;
        try {
            for (String line : Files.readAllLines(INDEXED_FILE, StandardCharsets.UTF_8)) {
                if (line.trim().equals(String.valueOf(bookId)))
                    return true;
            }
        } catch (IOException ignore) {
        }
        return false;
    }

    private static int countIndexedFromFile() {
        if (!Files.exists(INDEXED_FILE))
            return 0;
        try {
            return (int) Files.lines(INDEXED_FILE, StandardCharsets.UTF_8).filter(s -> !s.isBlank()).count();
        } catch (IOException e) {
            return 0;
        }
    }
}

