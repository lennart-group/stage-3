package bigdatastage3;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.cp.lock.FencedLock;

import java.util.Collection;
import java.util.List;

/**
 * In-Memory Inverted Index auf Basis einer Hazelcast MultiMap.
 *  Key   = Token/Term (String)
 *  Value = Dokument-ID (String)
 */
public class InMemoryInvertedIndex {

    private final HazelcastInstance hazelcast;
    private final MultiMap<String, String> invertedIndex;

    public InMemoryInvertedIndex(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
        this.invertedIndex = hazelcast.getMultiMap("inverted-index");
    }

    /**
     * Fügt ein Dokument mit seinen Tokens in den Index ein.
     */
    public void indexDocument(String docId, List<String> tokens) {
        if (docId == null || docId.isBlank() || tokens == null || tokens.isEmpty()) {
            return;
        }

        for (String token : tokens) {
            if (token == null || token.isBlank()) continue;

            String normalized = normalizeToken(token);

            // Optionaler Lock pro Term für sichere parallele Updates
            FencedLock lock = hazelcast
                    .getCPSubsystem()
                    .getLock("lock:inverted-index:" + normalized);

            lock.lock();
            try {
                invertedIndex.put(normalized, docId);
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Liefert alle Dokument-IDs, die den Term enthalten.
     */
    public Collection<String> search(String term) {
        if (term == null || term.isBlank()) {
            return List.of();
        }
        String normalized = normalizeToken(term);
        return invertedIndex.get(normalized);
    }

    private String normalizeToken(String token) {
        return token.toLowerCase().trim();
        // Hier könnte man später Stemming, Stopwords, etc. einbauen
    }
}
