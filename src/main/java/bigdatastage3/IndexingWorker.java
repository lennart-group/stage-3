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
 * JMS consumer for "document.ingested" events.
 *
 * For each message it:
 *  1. Reads the book from MongoDB.
 *  2. Calls IndexAPI.processBook(..) to update the inverted index.
 *  3. Emits a "document.indexed" event.
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
            if (!(message instanceof TextMessage)) {
                return;
            }

            String json = ((TextMessage) message).getText();
            Map<?, ?> payload = GSON.fromJson(json, Map.class);
            if (payload == null || !payload.containsKey("bookId")) {
                return;
            }

            // Gson deserializes numbers as Double by default => convert to int.
            int bookId = ((Double) payload.get("bookId")).intValue();

            Document d = booksCollection.find(Filters.eq("id", bookId)).first();
            if (d == null) {
                System.err.printf("⚠ Book %d not found for indexing%n", bookId);
                return;
            }

            String content = d.getString("content");
            if (content == null) {
                System.err.printf("⚠ Book %d has no content%n", bookId);
                return;
            }

            // Reuse existing indexing logic.
            IndexAPI.processBook(bookId, content);
            IndexAPI.lastUpdate = LocalDateTime.now();

            // Emit follow-up event to signal that indexing is done.
            broker.sendDocumentIndexed(bookId);

            System.out.printf("✅ Book %d indexed from broker event%n", bookId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
