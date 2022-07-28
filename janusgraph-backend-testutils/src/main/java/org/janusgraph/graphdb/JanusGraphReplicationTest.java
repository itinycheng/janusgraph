package org.janusgraph.graphdb;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.replication.JanusGraphReplication;
import org.junit.jupiter.api.Test;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;

public abstract class JanusGraphReplicationTest extends JanusGraphBaseTest {

    @Test
    public void smoke() throws BackendException {
        final JanusGraphReplication janusGraphReplication = new JanusGraphReplication();
        for (int i = 0; i < 1000; i++) {
            graph.addVertex();
        }
        graph.tx().commit();

        ModifiableConfiguration mainConfiguration =
            new ModifiableConfiguration(ROOT_NS, new CommonsConfiguration(graph.getConfiguration().getConfigurationAtOpen()),
                BasicConfiguration.Restriction.NONE);

        final ModifiableConfiguration replicaConfiguration = getReplicaConfiguration();

        janusGraphReplication.replicate(
            graph.getBackend(),
            mainConfiguration,
            replicaConfiguration);
    }

    public abstract ModifiableConfiguration getReplicaConfiguration();
}
