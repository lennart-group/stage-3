package bigdatastage3;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.Queue;

import org.bson.Document;

/**
 * Entry point for the indexing container.
 * Starts the JMS consumer and blocks forever.
 */
public class IndexingServiceApp {

  public static void main(String[] args) {

    try {
      // --- MongoDB connections ---
      MongoDatabase[] dbs = RepositoryConnection.connectToDB();
      MongoCollection<Document> booksCollection = dbs[0].getCollection("books");
      MongoDatabase indexDb = dbs[1];

      // --- Initialize indexing core ---
      IndexAPI.init(indexDb, booksCollection);

      // --- Message broker ---
      MessageBroker broker = new MessageBroker();

      // --- Workers ---
      IndexingWorker worker = new IndexingWorker(broker, booksCollection);

      ReindexingWorker reindexWorker = new ReindexingWorker(booksCollection, indexDb);

      // --- Subscriptions ---
      broker.subscribe(
          MessageBroker.QUEUE_DOC_INGESTED,
          worker);

      broker.subscribeTopic(
          MessageBroker.TOPIC_REINDEX_REQ,
          reindexWorker);

      System.out.println(
          "📥 IndexingService running (listening on document.ingested and reindex.request)");

      // Keep container alive
      Thread.currentThread().join();

    } catch (Exception e) {
      System.err.println("❌ IndexingService failed to start");
      e.printStackTrace();
      System.exit(1);
    }
  }
}
