package bigdatastage3;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import org.bson.Document;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

/**
 * Handles full reindexing triggered by reindex.request events.
 */
public class ReindexingWorker implements MessageListener {

  private final MongoCollection<Document> booksCollection;
  private final MongoDatabase indexDb;

  public ReindexingWorker(MongoCollection<Document> booksCollection,
      MongoDatabase indexDb) {
    this.booksCollection = booksCollection;
    this.indexDb = indexDb;
  }

  @Override
  public void onMessage(Message message) {
    System.out.println("🔄 Reindex request received");

    try {
      // 1. Drop all index collections (a–z buckets)
      clearIndex();

      // 2. Reset local index state
      Files.deleteIfExists(
          Path.of("control/indexed_books.txt"));

      // 3. Reindex all documents
      try (MongoCursor<Document> cursor = booksCollection.find().iterator()) {

        while (cursor.hasNext()) {
          Document doc = cursor.next();
          Integer id = doc.getInteger("id");
          String content = doc.getString("content");

          if (id != null && content != null && !content.isBlank()) {
            IndexAPI.processBook(id, content);
          }
        }
      }

      System.out.println("✅ Reindex completed successfully");

    } catch (Exception e) {
      System.err.println("❌ Reindex failed");
      e.printStackTrace();
    }
  }

  private void clearIndex() {
    Set<String> collections = indexDb.listCollectionNames().into(new HashSet<>());

    for (String name : collections) {
      if (name.length() == 1 && Character.isLetter(name.charAt(0))) {
        indexDb.getCollection(name).deleteMany(new Document());
      }
    }

    System.out.println("🧹 Inverted index cleared");
  }
}