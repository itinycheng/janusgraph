package org.janusgraph.graphdb;

import org.apache.commons.io.FileUtils;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.replication.JanusGraphReplication;

import java.io.*;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;

public class ReplicateTest {

    public static void main(String[] args) throws BackendException {
        final String mainBasePath = "/tmp/jg2";
        final String replicaBasePath = "/tmp";

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
                // .set(INDEX_BACKEND, "lucene", "search")
                // .set(INDEX_DIRECTORY, mainBasePath + "/index/", "search")
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
    /*

    [{Group=28, User=683, Configuration=25, vertex=1, SchemaVersion=1, EdgeSchema=92, UserSettings=2, StepExecutionData=2, ApplicationRoot=1, GlobalSettings=1, JobTemplate=118, JobExecutionHistory=1, Notification=5, ExecutionContextData=3, ConnectionTemplate=7, WidgetSchema=354, Project=25, MapSettings=1, AuditEntry=1196, Style=6, JobList=1, JobInstanceHistory=1, NodeSchema=146}]
[{owner=25, userSettings=2, HAS_GROUP=116}]

[{Group=28, User=683, Configuration=25, vertex=1, SchemaVersion=1, EdgeSchema=92, UserSettings=2, StepExecutionData=2, ApplicationRoot=1, GlobalSettings=1, JobTemplate=118, JobExecutionHistory=1, Notification=5, ExecutionContextData=3, ConnectionTemplate=7, WidgetSchema=354, Project=25, MapSettings=1, AuditEntry=1196, Style=6, JobList=1, JobInstanceHistory=1, NodeSchema=146}]
[{owner=25, userSettings=2, HAS_GROUP=116}]


     */
}
