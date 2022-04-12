package org.janusgraph;

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY;

@Disabled
public class BdbInterruptTest {
    JanusGraph graph;

    @BeforeEach
    void setUp() {
        final ModifiableConfiguration config = BerkeleyStorageSetup.getBerkeleyJEConfiguration();
        final String dir = config.get(STORAGE_DIRECTORY);
        FileUtils.deleteQuietly(new File(dir));
        graph = JanusGraphFactory.open(config);
    }

    @AfterEach
    void tearDown() {
        graph.close();
    }

    @RepeatedTest(10)
    public void test() throws InterruptedException {
        for (int i = 0; i < 50_000; i++) {
            graph.traversal()
                 .addV("V").property("a", "bb" + i).property("b", "bb" + i)
                 .addV("V").property("a", "bb" + i).property("b", "bb" + i)
                 .addV("V").property("a", "bb" + i).property("b", "bb" + i)
                 .iterate();
            if (i % 10_000 == 0) {
                graph.tx().commit();
            }
        }
        graph.tx().commit();

        final JanusGraphTransaction tx1 = graph.newTransaction();
        final JanusGraphTransaction tx2 = graph.newTransaction();

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            AtomicInteger i = new AtomicInteger();
            System.out.println(graph.traversal().V()
                                    .elementMap()
                                    .sideEffect(t -> {
                                        if (i.incrementAndGet() % 30_000 == 0) {
                                            System.out.println("count...");
                                        }
                                    })
                                    .count().next());
        }, executorService);

        TimeUnit.MILLISECONDS.sleep(2000);
        executorService.shutdownNow();

        try {
            future.get();
        } catch (ExecutionException e) {
            System.out.println("interrupted");
            Assertions.assertEquals(e.getCause().getClass(), TraversalInterruptedException.class);
        }

        try {
            Assertions.assertEquals(150000, graph.traversal().V().count().next());
        } catch (JanusGraphException e) {
            Assertions.fail("bdb should be reopened");
        }

        Assertions.assertEquals(150000, graph.traversal().V().count().next());
    }
}
