package bigdatastage3;

import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.UpdateResult;

import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.TextMessage;

import org.bson.Document;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

/**
 * Handles full reindexing triggered by reindex.request events.
 * Supports multiple Indexer containers working in parallel using batch
 * claiming.
 */
public class ReindexingWorker implements MessageListener {

  private static final int BATCH_SIZE = 5;
  private static final long LOCK_WAIT_MS = 500;
  private static final Duration LOCK_TIMEOUT = Duration.ofMinutes(5);

  private final MongoCollection<Document> booksCollection;
  private final MongoDatabase indexDb;

  public ReindexingWorker(MongoCollection<Document> booksCollection,
      MongoDatabase indexDb) {
    this.booksCollection = booksCollection;
    this.indexDb = indexDb;
  }

  @Override
  public void onMessage(Message message) {

    try {
      String json = ((TextMessage) message).getText();
      Map<?, ?> payload = new com.google.gson.Gson().fromJson(json, Map.class);

      if (payload == null || !payload.containsKey("runId")) {
        System.err.println("⚠ Invalid reindex message payload");
        return;
      }

      long startTime = System.currentTimeMillis();

      // 1️⃣ Acquire global clear lock
      String runId = payload.get("runId").toString();
      System.out.println("🔄 Reindex request received (runId: " + runId + ")");
      acquireClearLock(runId);

      // 2️⃣ Reindex in batches
      int totalIndexed = 0;

      while (true) {
        // Atomar BATCH claimen
        List<Document> batch = claimBatch();
        if (batch.isEmpty())
          break;

        for (Document doc : batch) {
          Integer id = doc.getInteger("id");
          String content = doc.getString("content");

          if (id != null && content != null && !content.isBlank()) {
            // Core indexing
            IndexAPI.processBook(id, content);
          }
        }

        totalIndexed += batch.size();
        System.out.printf("📦 Indexed batch of %d books (total indexed: %d)%n",
            batch.size(), totalIndexed);
      }

      long endTime = System.currentTimeMillis();
      System.out.printf("✅ Reindex completed successfully (%d books indexed) in %.2f s%n",
          totalIndexed, (endTime - startTime) / 1000.0);

    } catch (Exception e) {
      System.err.println("❌ Reindex failed");
      e.printStackTrace();
    }
  }

  // ---------------------- helper methods ----------------------

  /**
   * Acquire lock for clearing all index collections.
   * Only one container should actually clear the index.
   */
  private void acquireClearLock(String runId) throws Exception {
    MongoCollection<Document> lockCol = indexDb.getCollection("reindex_lock");

    while (true) {
      long now = System.currentTimeMillis();

      Document claimed = lockCol.findOneAndUpdate(
          Filters.eq("_id", runId),
          Updates.combine(
              Updates.setOnInsert("_id", runId),
              Updates.setOnInsert("createdAt", new Date()),
              Updates.setOnInsert("status", "LOCKED")),
          new FindOneAndUpdateOptions()
              .upsert(true)
              .returnDocument(ReturnDocument.BEFORE));

      // 🟢 Wir sind der erste → clear durchführen
      if (claimed == null) {
        try {
          System.out.println("🔐 Acquired reindex clear lock, clearing index...");
          clearIndex();
          resetIndexStatusForAllBooks();

          lockCol.updateOne(
              Filters.eq("_id", runId),
              Updates.set("status", "DONE"));

          System.out.println("🧹 Index cleared for reindex run " + runId);
        } catch (Exception e) {
          // wichtig: Lock freigeben oder markieren
          lockCol.updateOne(
              Filters.eq("_id", runId),
              Updates.set("status", "ERROR"));
          throw e;
        }
        return;
      }

      // 🔴 Wir sind NICHT der erste → warten
      Document current = lockCol.find(Filters.eq("_id", runId)).first();
      if (current != null) {
        String status = current.getString("status");
        Date createdAt = current.getDate("createdAt");
        System.out.println("⏳ Waiting for reindex lock (status: " + status + ")");

        if ("DONE".equals(status)) {
          return; // ✅ Index ist sauber
        }

        // ⏱ Lock-Timeout
        if (createdAt != null &&
            now - createdAt.getTime() > LOCK_TIMEOUT.toMillis()) {

          System.err.println("⚠️ Clear lock expired, retrying");
          lockCol.deleteOne(Filters.eq("_id", runId));
        }
      }

      Thread.sleep(LOCK_WAIT_MS);
    }
  }

  /**
   * Atomar BATCH von Büchern claimen, die noch nicht indexiert sind.
   */
  private List<Document> claimBatch() {
    List<Document> batch = new ArrayList<>();

    for (int i = 0; i < BATCH_SIZE; i++) {
      Document claimed = booksCollection.findOneAndUpdate(
          Filters.or(
              Filters.exists("indexStatus", false),
              Filters.eq("indexStatus", "NEW")),
          Updates.combine(
              Updates.set("indexStatus", "INDEXING"),
              Updates.set("indexStartedAt", new Date())),
          new FindOneAndUpdateOptions()
              .sort(Sorts.ascending("id")) // 🔑 wichtig!
              .returnDocument(ReturnDocument.AFTER));

      if (claimed == null) {
        break; // nichts mehr zu holen
      }

      batch.add(claimed);
    }

    return batch;
  }

  /**
   * Delete all single-letter index collections (a–z).
   */
  private void clearIndex() {
    Set<String> collections = indexDb.listCollectionNames().into(new HashSet<>());

    for (String name : collections) {
      if (name.length() == 1 && Character.isLetter(name.charAt(0))) {
        indexDb.getCollection(name).deleteMany(new Document());
      }
    }

    System.out.println("🧹 Inverted index cleared");
  }

  private void resetIndexStatusForAllBooks() {
    UpdateResult result = booksCollection.updateMany(
        new Document(), // alle Bücher
        Updates.combine(
            Updates.set("indexStatus", "NEW"),
            Updates.unset("indexStartedAt"),
            Updates.unset("indexError")));

    System.out.printf("🔁 Reset indexStatus to NEW for %d books%n",
        result.getModifiedCount());
  }

}
