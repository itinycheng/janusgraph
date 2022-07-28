package org.janusgraph.graphdb.berkeleyje;

import org.janusgraph.BerkeleyStorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphReplicationTest;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

public class BerkeleyReplicationTest extends JanusGraphReplicationTest {
    @TempDir
    File replicaDirectory;

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration modifiableConfiguration = BerkeleyStorageSetup.getBerkeleyJEConfiguration();
        return modifiableConfiguration.getConfiguration();
    }

    @Override
    public ModifiableConfiguration getReplicaConfiguration() {
        return BerkeleyStorageSetup.getBerkeleyJEConfiguration(replicaDirectory.getAbsolutePath());
    }
}
