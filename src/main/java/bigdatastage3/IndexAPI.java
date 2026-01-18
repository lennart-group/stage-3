package bigdatastage3;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;

import org.bson.Document;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Core indexing logic.
 * No REST, no JMS, no container lifecycle.
 * Used exclusively by IndexingWorker.
 */
public class IndexAPI {

  private static MongoDatabase indexDb;
  private static MongoCollection<Document> booksCollection;

  public static LocalDateTime lastUpdate = null;

  /** Must be called once during service startup */
  public static void init(MongoDatabase indexDatabase, MongoCollection<Document> booksCol) throws IOException {
    indexDb = indexDatabase;
    booksCollection = booksCol;
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

    System.out.println("🔍 Indexing book " + bookId);

    Set<String> terms = tokenize(text);

    // Bulk update MongoDB inverted index
    updateMongoInvertedIndexBulk(terms, bookId);

    markIndexed(bookId);
    lastUpdate = LocalDateTime.now();

    System.out.printf(
        "✅ Indexed book %d (%d unique terms)%n",
        bookId, terms.size());
  }

  // ---------- internal helpers ----------

  private static Set<String> tokenize(String text) {
    Set<String> tokens = new HashSet<>();
    if (text == null)
      return tokens;

    Matcher m = Pattern.compile("\\b[a-z]{2,}\\b")
        .matcher(text.toLowerCase());

    while (m.find()) {
      tokens.add(m.group());
    }
    return tokens;
  }

  private static void updateMongoInvertedIndexBulk(Set<String> terms, int bookId) {
    // Map bucket -> List of term updates
    Map<String, List<WriteModel<Document>>> bucketWrites = new HashMap<>();

    for (String term : terms) {
      String bucket = term.substring(0, 1);
      bucketWrites.computeIfAbsent(bucket, k -> new ArrayList<>())
          .add(new UpdateOneModel<>(
              Filters.eq("term", term),
              Updates.addToSet("postings", bookId),
              new UpdateOptions().upsert(true)));
    }

    // Write each bucket in bulk
    for (Map.Entry<String, List<WriteModel<Document>>> entry : bucketWrites.entrySet()) {
      String bucket = entry.getKey();
      List<WriteModel<Document>> writes = entry.getValue();
      if (!writes.isEmpty()) {
        MongoCollection<Document> col = indexDb.getCollection(bucket);
        col.bulkWrite(writes, new BulkWriteOptions().ordered(false));
      }
    }
  }

  private static void markIndexed(int bookId) throws IOException {
    // Update book as done
            booksCollection.updateOne(
                Filters.eq("id", bookId),
                Updates.combine(
                    Updates.set("indexStatus", "DONE"),
                    Updates.set("indexFinishedAt", new Date())));
    System.out.println("Marked book " + bookId + " as indexed");
  }

  private static boolean alreadyIndexed(int bookId) {
    return booksCollection.find(
        Filters.and(
            Filters.eq("id", bookId),
            Filters.eq("indexStatus", "DONE")))
        .first() != null;
  }
}
