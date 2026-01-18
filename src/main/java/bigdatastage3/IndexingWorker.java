package bigdatastage3;

import com.google.gson.Gson;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import java.util.Date;
import java.time.LocalDateTime;

import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.TextMessage;
import org.bson.Document;

import java.util.Map;

/**
 * JMS consumer for document.ingested events.
 *
 * Flow:
 * 1. Receive event from ActiveMQ
 * 2. Load document from datalake (MongoDB)
 * 3. Update in-memory / distributed inverted index
 * 4. Emit document.indexed event
 */
public class IndexingWorker implements MessageListener {

  private static final Gson GSON = new Gson();

  private final MessageBroker broker;
  private final MongoCollection<Document> booksCollection;

  public IndexingWorker(MessageBroker broker,
                        MongoCollection<Document> booksCollection) {
    this.broker = broker;
    this.booksCollection = booksCollection;
  }

  @Override
  public void onMessage(Message message) {

    Integer bookId = null;

    try {
      if (!(message instanceof TextMessage)) {
        return;
      }

      String json = ((TextMessage) message).getText();
      Map<?, ?> payload = GSON.fromJson(json, Map.class);

      if (payload == null || !payload.containsKey("bookId")) {
        System.err.println("⚠ Invalid indexing message payload");
        return;
      }

      bookId = ((Double) payload.get("bookId")).intValue();

      // 🔒 Atomic claim
      Document claimed = booksCollection.findOneAndUpdate(
          Filters.and(
              Filters.eq("id", bookId),
              Filters.or(
                  Filters.exists("indexStatus", false),
                  Filters.eq("indexStatus", "NEW")
              )
          ),
          Updates.combine(
              Updates.set("indexStatus", "INDEXING"),
              Updates.set("indexStartedAt", new Date())
          ),
          new FindOneAndUpdateOptions()
              .returnDocument(ReturnDocument.BEFORE)
      );

      if (claimed == null) {
        System.out.printf("⏭ Book %d already indexed or indexing%n", bookId);
        return;
      }

      Document doc = booksCollection.find(Filters.eq("id", bookId)).first();
      if (doc == null) {
        throw new IllegalStateException("Book not found");
      }

      String content = doc.getString("content");
      if (content == null || content.isBlank()) {
        throw new IllegalStateException("Empty content");
      }

      System.out.println("📥 Indexing book " + bookId);
      // 🔨 Index
      IndexAPI.processBook(bookId, content);
      IndexAPI.lastUpdate = LocalDateTime.now();

      // ✅ Mark done
      booksCollection.updateOne(
          Filters.eq("id", bookId),
          Updates.set("indexStatus", "DONE")
      );

      broker.sendDocumentIndexed(bookId);
      System.out.printf("✅ Book %d indexed%n", bookId);

    } catch (Exception e) {
      System.err.println("❌ Indexing failed for book " + bookId);
      e.printStackTrace();

      if (bookId != -1) {
        booksCollection.updateOne(
            Filters.eq("id", bookId),
            Updates.combine(
                Updates.set("indexStatus", "ERROR"),
                Updates.set("indexError", e.getMessage())
            )
        );
      }
    }
  }
}

