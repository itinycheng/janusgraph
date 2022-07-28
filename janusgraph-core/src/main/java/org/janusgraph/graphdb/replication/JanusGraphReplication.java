package org.janusgraph.graphdb.replication;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJob;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.builder.GraphDatabaseConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.janusgraph.diskstorage.Backend.EDGESTORE_NAME;
import static org.janusgraph.diskstorage.Backend.INDEXSTORE_NAME;
import static org.janusgraph.diskstorage.Backend.LOCK_STORE_SUFFIX;
import static org.janusgraph.diskstorage.Backend.SYSTEM_MGMT_LOG_NAME;
import static org.janusgraph.diskstorage.Backend.SYSTEM_TX_LOG_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDS_STORE_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;

public class JanusGraphReplication {

    public static void main(String[] args) throws BackendException {
        final String mainBasePath = args[1];
        final String replicaBasePath = args[2];

        /*
         config1
         config2 (change data directory, index stay same)

         run

         backup old directory, rename new directory and reopen
         */
        final String mainDirectory = mainBasePath + "/datagraph.db/";
        ModifiableConfiguration mainConfiguration =
            buildGraphConfiguration()
                .set(STORAGE_DIRECTORY, mainDirectory)
                .set(INDEX_BACKEND, "lucene", "search")
                .set(INDEX_DIRECTORY, mainBasePath + "/index/", "search")
                .set(STORAGE_BACKEND, "berkeleyje");

        // final JanusGraph graph = JanusGraphFactory.open(mainConfiguration.getConfiguration());
        // System.out.println("Dump main");
        // graph.traversal().V().elementMap().forEachRemaining(em -> {
        // System.out.println(em);
        // });
        // System.out.println(graph.traversal().V().groupCount().by(__.label()).toList());
        // System.out.println(graph.traversal().E().groupCount().by(__.label()).toList());
        // System.out.println("Done");
        // graph.traversal().addV().property("p1", "b").next();
        // graph.traversal().addV().property("p2", 10).next();
        // graph.traversal().addV().property("p3", new Date()).next();
        // graph.traversal().tx().commit();
        // graph.close();

        final String replicaDirectory = replicaBasePath + "/replicate.db";
        FileUtils.deleteQuietly(new File(replicaDirectory));
        ModifiableConfiguration replicaConfiguration =
            buildGraphConfiguration()
                .set(STORAGE_DIRECTORY, replicaDirectory)
                .set(INDEX_BACKEND, "lucene", "search")
                .set(INDEX_DIRECTORY, mainBasePath + "/index/", "search")
                .set(STORAGE_BACKEND, "org.janusgraph.diskstorage.rocksdb.RocksDbStoreManager");

        final JanusGraphReplication janusGraphReplication = new JanusGraphReplication();
        janusGraphReplication.replicate(mainConfiguration, replicaConfiguration);

        // final JanusGraph replicaGraph = JanusGraphFactory.open(replicaConfiguration.getConfiguration());
        // final JanusGraphManagement mgmt = replicaGraph.openManagement();
        // System.out.println(mgmt.printSchema());
        // replicaGraph.configuration().getKeys().forEachRemaining(k -> {
        //     System.out.println(k + ": " + replicaGraph.configuration().getString(k));
        // });
        // mgmt.rollback();
        // replicaGraph.traversal().V().elementMap().forEachRemaining(em -> {
        // System.out.println(em);
        // });
        // System.out.println(replicaGraph.traversal().V().groupCount().by(__.label()).toList());
        // System.out.println(replicaGraph.traversal().E().groupCount().by(__.label()).toList());

        // replicaGraph.close();
    }

    public void replicate(Backend mainBackend, ModifiableConfiguration mainConfiguration, ModifiableConfiguration replicaConfiguration) throws BackendException {
        // Validate conflict options: host, directory, index, namespace, identifier namespace?
        // Reindex all mixed index if exists or just copy for lucene?
        // Fix path?

        // Naive approach - just copy all stores by ScanJob

        final GraphDatabaseConfiguration replicaGraphConfig = new GraphDatabaseConfigurationBuilder().build(replicaConfiguration.getConfiguration());

        Backend replicaBackend = replicaGraphConfig.getBackend();

        replicaBackend.clearStorage();
        replicaBackend.close();
        replicaBackend = replicaGraphConfig.getBackend();

        try {
            String mainLockStoreSuffix;
            if (mainBackend.getStoreFeatures().hasLocking()) {
                mainLockStoreSuffix = "";
            } else {
                mainLockStoreSuffix = LOCK_STORE_SUFFIX;
            }
            String replicaLockStoreSuffix;
            if (replicaBackend.getStoreFeatures().hasLocking()) {
                replicaLockStoreSuffix = "";
            } else {
                replicaLockStoreSuffix = LOCK_STORE_SUFFIX;
            }

            Map<String, String> storeMapping = new LinkedHashMap<>();
            storeMapping.put(EDGESTORE_NAME + mainLockStoreSuffix, EDGESTORE_NAME + replicaLockStoreSuffix);
            // WARN: Possible inconsistency when copy index store during mutations
            // But we can't ignore index store because it contains index for internal schema too
            storeMapping.put(INDEXSTORE_NAME + mainLockStoreSuffix, INDEXSTORE_NAME + replicaLockStoreSuffix);
            storeMapping.put(SYSTEM_PROPERTIES_STORE_NAME + mainLockStoreSuffix, SYSTEM_PROPERTIES_STORE_NAME + replicaLockStoreSuffix);
            storeMapping.put(SYSTEM_TX_LOG_NAME, SYSTEM_TX_LOG_NAME);
            storeMapping.put(SYSTEM_MGMT_LOG_NAME, SYSTEM_MGMT_LOG_NAME);
            storeMapping.put(mainConfiguration.get(IDS_STORE_NAME), replicaConfiguration.get(IDS_STORE_NAME));

            //     public static final String MANAGEMENT_LOG = "janusgraph";
            //     public static final String TRANSACTION_LOG = "tx";
            //     public static final String USER_LOG = "user";
            //     public static final String USER_LOG_PREFIX = "ulog_";

            for (final Map.Entry<String, String> entry : storeMapping.entrySet()) {
               final ScanMetrics scanMetrics = mainBackend.buildStoreScanJob(entry.getKey())
                                                           .setNumProcessingThreads(1)
                                                           .setWorkBlockSize(50_000)
                                                           .setJob(new ReplicateStore(replicaBackend, entry.getValue())).execute()
                                                           .get();
                Preconditions.checkArgument(scanMetrics.get(ScanMetrics.Metric.FAILURE) == 0, "Replication failed, show logs for more info");
            }
            replicaBackend.close();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void replicate(ModifiableConfiguration mainConfiguration, ModifiableConfiguration replicaConfiguration) throws BackendException {
        final GraphDatabaseConfiguration mainGraphConfig = new GraphDatabaseConfigurationBuilder().build(mainConfiguration.getConfiguration());
        final Backend mainBackend = mainGraphConfig.getBackend();
        replicate(mainBackend, mainConfiguration, replicaConfiguration);
        mainBackend.close();
    }

    public static class ReplicateStore implements ScanJob {
        private static final Logger log = LoggerFactory.getLogger(Backend.class);
        private static final SliceQuery EVERYTHING_QUERY = new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(128));

        private final Backend replicaBackend;
        private final String storeName;
        private final KeyColumnValueStoreManager replicaStoreManager;
        private StoreTransaction replicaStoreTx;
        private Map<StaticBuffer, KCVMutation> mutations;

        public ReplicateStore(final Backend replicaBackend, String storeName) {
            this.replicaBackend = replicaBackend;
            this.storeName = storeName;
            this.replicaStoreManager = (KeyColumnValueStoreManager) replicaBackend.getStoreManager();
        }

        @Override
        public void workerIterationStart(final Configuration jobConfiguration, final Configuration graphConfiguration, final ScanMetrics metrics) {
            ScanJob.super.workerIterationStart(jobConfiguration, graphConfiguration, metrics);
            mutations = new HashMap<>();
            StandardBaseTransactionConfig.Builder txBuilder = new StandardBaseTransactionConfig.Builder();
            txBuilder.timestampProvider(TimestampProviders.MICRO);
            try {
                replicaStoreTx = replicaStoreManager.beginTransaction(txBuilder.build());
            } catch (BackendException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void process(final StaticBuffer key, final Map<SliceQuery, EntryList> entries, final ScanMetrics metrics) {
            final EntryList additions = entries.get(EVERYTHING_QUERY);
            Preconditions.checkArgument(additions.size() > 0);
            mutations.put(key, new KCVMutation(additions, Collections.emptyList()));
        }

        @Override
        public void workerIterationEnd(final ScanMetrics metrics) {
            try {
                replicaStoreManager.mutateMany(Collections.singletonMap(storeName, mutations), replicaStoreTx);
                replicaStoreTx.commit();
                log.debug("Add " + mutations.size() + " into " + storeName);
            } catch (BackendException e) {
                throw new RuntimeException(e);
            }
            mutations = null;
            ScanJob.super.workerIterationEnd(metrics);
        }

        @Override
        public List<SliceQuery> getQueries() {
            return Lists.newArrayList(EVERYTHING_QUERY);
        }

        @Override
        public ScanJob clone() {
            return new ReplicateStore(replicaBackend, storeName);
        }
    }
}
