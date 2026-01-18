package bigdatastage3;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;

import jakarta.jms.JMSException;

import org.bson.Document;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IndexAPI {

    private static MongoDatabase indexDb;
    private static MongoCollection<Document> booksCollection;

    private static MessageBroker broker;
    public static LocalDateTime lastUpdate = LocalDateTime.now();

    /** Must be called once during service startup */
    public static void init(MongoDatabase indexDatabase, MongoCollection<Document> booksCol) throws IOException {
        indexDb = indexDatabase;
        booksCollection = booksCol;
        try {
          broker = new MessageBroker();
        } catch (JMSException e) {
          System.err.println("❌ Failed to initialize MessageBroker in IndexAPI");
          e.printStackTrace();
        }
        System.out.println("✅ IndexAPI initialized");
    }

    /**
     * Tokenizes the document text and updates the distributed inverted index.
     */
    public static void processBook(int bookId, String text) {
        if (alreadyIndexed(bookId)) {
            System.out.printf("ℹ Book %d already indexed, skipping%n", bookId);
            return;
        }

        try {
            System.out.println("🔍 Indexing book " + bookId);

            Set<String> terms = tokenize(text);

            // Bulk update MongoDB inverted index with retry
            boolean success = retryBulkUpdate(terms, bookId, 3);

            if (!success) {
                throw new RuntimeException("Failed to update MongoDB after retries");
            }

            markIndexed(bookId);
            lastUpdate = LocalDateTime.now();

            System.out.printf("✅ Indexed book %d (%d unique terms)%n", bookId, terms.size());

        } catch (Exception e) {
            System.err.printf("❌ Indexing book %d failed: %s%n", bookId, e.getMessage());

            // Set FAILED status
            booksCollection.updateOne(
                Filters.eq("id", bookId),
                Updates.set("indexStatus", "FAILED")
            );

            // Notify broker for retry
            try {
                broker.sendDocumentIngested(bookId);
            } catch (Exception ex) {
                System.err.printf("❌ Broker callback failed for book %d: %s%n", bookId, ex.getMessage());
            }
        }
    }

    // ---------------------- Internal Helpers ----------------------

    private static Set<String> tokenize(String text) {
        Set<String> tokens = new HashSet<>();
        if (text == null) return tokens;

        Matcher m = Pattern.compile("\\b[a-z]{2,}\\b").matcher(text.toLowerCase());
        while (m.find()) tokens.add(m.group());
        return tokens;
    }

    private static boolean retryBulkUpdate(Set<String> terms, int bookId, int maxRetries) {
        int attempt = 0;
        while (attempt < maxRetries) {
            try {
                updateMongoInvertedIndexBulk(terms, bookId);
                return true;
            } catch (Exception e) {
                attempt++;
                System.err.printf("⚠ Attempt %d failed for book %d: %s%n", attempt, bookId, e.getMessage());
                try { Thread.sleep(1000L * attempt); } catch (InterruptedException ignored) {}
            }
        }
        return false;
    }

    private static void updateMongoInvertedIndexBulk(Set<String> terms, int bookId) {
        Map<String, List<WriteModel<Document>>> bucketWrites = new HashMap<>();

        for (String term : terms) {
            String bucket = term.substring(0, 1);
            bucketWrites.computeIfAbsent(bucket, k -> new ArrayList<>())
                .add(new UpdateOneModel<>(
                    Filters.eq("term", term),
                    Updates.addToSet("postings", bookId),
                    new UpdateOptions().upsert(true)
                ));
        }

        for (Map.Entry<String, List<WriteModel<Document>>> entry : bucketWrites.entrySet()) {
            String bucket = entry.getKey();
            List<WriteModel<Document>> writes = entry.getValue();
            if (!writes.isEmpty()) {
                MongoCollection<Document> col = indexDb.getCollection(bucket);
                col.bulkWrite(writes, new BulkWriteOptions().ordered(false));
            }
        }
    }

    private static void markIndexed(int bookId) {
        booksCollection.updateOne(
            Filters.eq("id", bookId),
            Updates.combine(
                Updates.set("indexStatus", "DONE"),
                Updates.set("indexFinishedAt", new Date())
            )
        );
        System.out.println("Marked book " + bookId + " as indexed");
    }

    private static boolean alreadyIndexed(int bookId) {
        return booksCollection.find(
            Filters.and(
                Filters.eq("id", bookId),
                Filters.eq("indexStatus", "DONE")
            )
        ).first() != null;
    }
}
