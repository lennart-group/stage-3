package bigdatastage3;

import com.google.gson.Gson;
import jakarta.jms.*;
import jakarta.jms.ConnectionFactory;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * 
 *
 * Queue-ji / events:
 *  - document.ingested
 *  - document.indexed
 *  - reindex.request
 */
public class MessageBroker implements Closeable {

    public static final String QUEUE_DOC_INGESTED = "document.ingested";
    public static final String QUEUE_DOC_INDEXED  = "document.indexed";
    public static final String QUEUE_REINDEX_REQ  = "reindex.request";

    private static final Gson GSON = new Gson();

    private final Connection connection;
    private final Session session;

    public MessageBroker() throws JMSException {
        String brokerUrl = System.getenv("BROKER_URL");

        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        this.connection = factory.createConnection();
        this.connection.start();

        // brez transakcij, auto-ack
        this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    // ---------- PRODUCERS ----------

    public void sendDocumentIngested(int bookId) throws JMSException {
        Destination queue = session.createQueue(QUEUE_DOC_INGESTED);
        sendJson(queue, Map.of(
                "bookId", bookId,
                "event", "document.ingested"
        ));
    }

    public void sendDocumentIndexed(int bookId) throws JMSException {
        Destination queue = session.createQueue(QUEUE_DOC_INDEXED);
        sendJson(queue, Map.of(
                "bookId", bookId,
                "event", "document.indexed"
        ));
    }

    public void sendReindexRequest() throws JMSException {
        Destination queue = session.createQueue(QUEUE_REINDEX_REQ);
        sendJson(queue, Map.of(
                "event", "reindex.request"
        ));
    }

    private void sendJson(Destination dest, Map<String, Object> payload) throws JMSException {
        MessageProducer producer = session.createProducer(dest);
        TextMessage msg = session.createTextMessage(GSON.toJson(payload));
        producer.send(msg);
        producer.close();
    }

    // ---------- CONSUMERS ----------

    public MessageConsumer subscribe(String queueName, MessageListener listener) throws JMSException {
        Destination queue = session.createQueue(queueName);
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(listener);
        return consumer;
    }

    @Override
    public void close() throws IOException {
        try {
            if (session != null) session.close();
            if (connection != null) connection.close();
        } catch (JMSException e) {
            throw new IOException(e);
        }
    }
}
