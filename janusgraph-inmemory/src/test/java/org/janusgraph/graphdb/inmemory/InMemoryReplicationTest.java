package org.janusgraph.graphdb.inmemory;

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphReplicationTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;

public class InMemoryReplicationTest extends JanusGraphReplicationTest {
    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        return config.getConfiguration();
    }

    @Override
    public ModifiableConfiguration getReplicaConfiguration() {
        return GraphDatabaseConfiguration.buildGraphConfiguration()
                                         .set(STORAGE_BACKEND, "inmemory");
    }
}
