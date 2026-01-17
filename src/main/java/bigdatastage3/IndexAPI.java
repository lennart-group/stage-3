package bigdatastage3;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.bson.Document;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Core indexing logic.
 * No REST, no JMS, no container lifecycle.
 * Used exclusively by IndexingWorker.
 */
public class IndexAPI {

    private static final Path CONTROL_DIR = Paths.get("control");
    private static final Path INDEXED_FILE = CONTROL_DIR.resolve("indexed_books.txt");

    private static MongoDatabase indexDb;

    public static LocalDateTime lastUpdate = null;

    /** Must be called once during service startup */
    public static void init(MongoDatabase indexDatabase) throws IOException {
        indexDb = indexDatabase;
        ensureControlDir();
        System.out.println("✅ IndexAPI initialized");
    }

    /**
     * Tokenizes the document text and updates the distributed inverted index.
     */
    public static void processBook(int bookId, String text) throws Exception {
        if (alreadyIndexed(bookId)) {
            System.out.printf("ℹ Book %d already indexed, skipping%n", bookId);
            return;
        }

        Set<String> terms = tokenize(text);

        for (String term : terms) {
            updateMongoInvertedIndex(term, bookId);
        }

        markIndexed(bookId);
        lastUpdate = LocalDateTime.now();

        System.out.printf(
            "✅ Indexed book %d (%d unique terms)%n",
            bookId, terms.size()
        );
    }

    // ---------- internal helpers ----------

    private static Set<String> tokenize(String text) {
        Set<String> tokens = new HashSet<>();
        if (text == null) return tokens;

        Matcher m = Pattern.compile("\\b[a-z]{2,}\\b")
                .matcher(text.toLowerCase());

        while (m.find()) {
            tokens.add(m.group());
        }
        return tokens;
    }

    private static void updateMongoInvertedIndex(String term, int bookId) {
        String bucket = term.substring(0, 1);
        MongoCollection<Document> col = indexDb.getCollection(bucket);

        col.updateOne(
            Filters.eq("term", term),
            Updates.addToSet("postings", bookId),
            new UpdateOptions().upsert(true)
        );
    }

    private static void ensureControlDir() throws IOException {
        if (!Files.exists(CONTROL_DIR)) {
            Files.createDirectories(CONTROL_DIR);
        }
    }

    private static void markIndexed(int bookId) throws IOException {
        try (BufferedWriter bw = Files.newBufferedWriter(
                INDEXED_FILE,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND)) {
            bw.write(String.valueOf(bookId));
            bw.newLine();
        }
    }

    private static boolean alreadyIndexed(int bookId) {
        if (!Files.exists(INDEXED_FILE)) return false;
        try {
            return Files.readAllLines(INDEXED_FILE, StandardCharsets.UTF_8)
                    .stream()
                    .anyMatch(line -> line.trim().equals(String.valueOf(bookId)));
        } catch (IOException e) {
            return false;
        }
    }
}
