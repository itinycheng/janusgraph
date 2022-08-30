package org.janusgraph.diskstorage.lucene;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DROP_ON_CLEAR;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

public class InmemoryLuceneConsistencyTest extends LuceneIndexConsistencyTest {
    @Override
    public void setUp() throws Exception {
        config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        config.set(INDEX_BACKEND, "lucene", BACKING_INDEX);
        config.set(INDEX_DIRECTORY, tempDir.resolve("lucene").toString(), BACKING_INDEX);
        config.set(DROP_ON_CLEAR, false);
        WriteConfiguration configuration = config.getConfiguration();
        graph = JanusGraphFactory.open(configuration);
        createIndex(VERTEX_LABEL);
    }
}
