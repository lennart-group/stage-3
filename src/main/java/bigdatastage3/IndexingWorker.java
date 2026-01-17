package bigdatastage3;

import com.google.gson.Gson;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.TextMessage;
import org.bson.Document;

import java.time.LocalDateTime;
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
    try {
      System.out.println("Received message for indexing");
      System.out.flush();
      if (!(message instanceof TextMessage)) {
        return;
      }

      String json = ((TextMessage) message).getText();
      Map<?, ?> payload = GSON.fromJson(json, Map.class);

      if (payload == null || !payload.containsKey("bookId")) {
        System.err.println("⚠ Invalid indexing message payload");
        return;
      }

      // Gson parses numbers as Double
      int bookId = ((Double) payload.get("bookId")).intValue();

      // Retrieve document from datalake
      Document doc = booksCollection
          .find(Filters.eq("id", bookId))
          .first();

      if (doc == null) {
        System.err.printf(
            "⚠ Book %d not found in datalake%n", bookId);
        return;
      }

      String content = doc.getString("content");
      if (content == null || content.isBlank()) {
        System.err.printf(
            "⚠ Book %d has no indexable content%n", bookId);
        return;
      }

      // Core indexing logic
      System.out.println("Indexing book: " + bookId + "...");
      IndexAPI.processBook(bookId, content);
      IndexAPI.lastUpdate = LocalDateTime.now();

      // Notify observers
      broker.sendDocumentIndexed(bookId);
      System.out.println("📨 Sent document.indexed for book " + bookId);

      System.out.printf(
          "✅ Book %d indexed via broker event%n", bookId);

    } catch (Exception e) {
      System.err.println("❌ Error while processing indexing event");
      e.printStackTrace();
    }
  }
}
