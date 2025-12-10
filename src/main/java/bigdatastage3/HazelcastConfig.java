package bigdatastage3;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Erzeugt und verwaltet die Hazelcast-Instanz für den verteilten In-Memory-Index.
 */
public class HazelcastConfig {

    private static HazelcastInstance INSTANCE;

    public static HazelcastInstance getHazelcastInstance() {
        if (INSTANCE == null) {
            Config config = new Config();

            // Cluster-Name (muss bei allen Knoten gleich sein)
            config.setClusterName("search-cluster");

            // Netzwerk-Einstellungen
            config.getNetworkConfig()
                    .setPort(5701)
                    .setPortAutoIncrement(true);

            // Konfiguration für die Inverted-Index-Map
            MapConfig mapCfg = new MapConfig("inverted-index")
                    .setBackupCount(2)       // synchrone Backups
                    .setAsyncBackupCount(1); // asynchrones Backup

            config.addMapConfig(mapCfg);

            INSTANCE = Hazelcast.newHazelcastInstance(config);
        }
        return INSTANCE;
    }

    public static void shutdown() {
        if (INSTANCE != null) {
            INSTANCE.shutdown();
            INSTANCE = null;
        }
    }
}
