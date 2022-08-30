package org.janusgraph.diskstorage.lucene;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.StorageSetup;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;

@BenchmarkMode(Mode.Throughput)
@Fork(jvmArgsAppend = "-Xmx256M")
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class LuceneIndexBenchmark {
    private static final String BACKING_INDEX = "search";

    JanusGraph graph;

    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        config.set(INDEX_BACKEND, "lucene", BACKING_INDEX);
        config.set(INDEX_DIRECTORY, StorageSetup.getHomeDir("lucene"), BACKING_INDEX);
        return config.getConfiguration();
    }

    @Setup
    public void setUp() throws Exception {
        graph = JanusGraphFactory.open(getConfiguration());
        createIndex("vertex1");
        createIndex("vertex2");
        createIndex("vertex3");
    }

    private void createIndex(String labelName) {
        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey(labelName + "_name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        VertexLabel label = mgmt.makeVertexLabel(labelName).make();
        mgmt.buildIndex(labelName, Vertex.class).addKey(name).indexOnly(label).buildMixedIndex(BACKING_INDEX);
        mgmt.commit();
    }

    @TearDown
    public void tearDown() {
        graph.close();
    }

    @Threads(100)
    @Benchmark
    public void createVertices() {
        Object id1 = graph.traversal().addV("vertex1").property("vertex1_name", RandomStringUtils.randomAlphabetic(4, 20)).id().next();
        Object id2 = graph.traversal().addV("vertex2").property("vertex2_name", RandomStringUtils.randomAlphabetic(4, 20)).id().next();
        Object id3 = graph.traversal().addV("vertex3").property("vertex3_name", RandomStringUtils.randomAlphabetic(4, 20)).id().next();
        graph.traversal().tx().commit();
        graph.traversal().V(id1).property("vertex1_name", RandomStringUtils.randomAlphabetic(4, 20)).id().next();
        graph.traversal().V(id2).property("vertex2_name", RandomStringUtils.randomAlphabetic(4, 20)).id().next();
        graph.traversal().V(id3).property("vertex3_name", RandomStringUtils.randomAlphabetic(4, 20)).id().next();
        graph.traversal().tx().commit();
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
            .include(LuceneIndexBenchmark.class.getSimpleName())
            .forks(1)
            .warmupIterations(0)
            .measurementIterations(10)
            .measurementTime(TimeValue.seconds(10))
            .timeout(TimeValue.minutes(60))
            .build();
        new Runner(options).run();
    }
}
