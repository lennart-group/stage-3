package bigdatastage3;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class HazelcastConfig {

    private static HazelcastInstance INSTANCE;

    public static HazelcastInstance getHazelcastInstance() {
        if (INSTANCE == null) {

            Config config = new Config();
            config.setClusterName("search-cluster");

            // ---------- NETWORK ----------
            NetworkConfig network = config.getNetworkConfig();
            network.setPort(5701).setPortAutoIncrement(true);

            JoinConfig join = network.getJoin();
            join.getMulticastConfig().setEnabled(false);
            TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
            tcpIpConfig.setEnabled(true);

            // 🔹 Docker Multi-Instance Discovery
            // Docker erzeugt automatisch DNS wie: search-1, search-2, search-3
            // Für IndexAPI z.B.: index-1, index-2
            tcpIpConfig.addMember("search")   // generischer Service-Name
                      .addMember("index");   // Index-Instanzen, falls benötigt

            // ---------- MAP CONFIG ----------
            MapConfig mapCfg = new MapConfig("inverted-index")
                    .setBackupCount(2)
                    .setAsyncBackupCount(1);

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
