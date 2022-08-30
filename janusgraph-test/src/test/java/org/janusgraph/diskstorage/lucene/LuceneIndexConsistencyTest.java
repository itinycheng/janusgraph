package org.janusgraph.diskstorage.lucene;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.BaseTransactionConfigurable;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.diskstorage.indexing.StandardKeyInformation;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.util.encoding.LongEncoding;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public abstract class LuceneIndexConsistencyTest {
    protected static final String BACKING_INDEX = "search";
    protected static final String VERTEX_LABEL = "testlabel";
    private static final String PROPERTY_NAME = "name";
    JanusGraph graph;

    @TempDir
    static Path tempDir;
    private final Map<Object, Map<Integer, Integer>> operationLog = new ConcurrentHashMap<>();
    protected ModifiableConfiguration config;

    protected void createIndex(String labelName) {
        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey(PROPERTY_NAME).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        VertexLabel label = mgmt.makeVertexLabel(labelName).make();
        mgmt.buildIndex(labelName, Vertex.class).addKey(name).indexOnly(label).buildMixedIndex(BACKING_INDEX);
        mgmt.commit();
    }

    protected abstract void setUp() throws Exception;

    @BeforeEach
    public void init() throws Exception {
        setUp();
    }

    @AfterEach
    public void tearDown() {
        graph.close();
    }

    private String populate(int iterationLimit, int minVertexLimit, int maxVertexLimit,
                            int threadLimit) throws InterruptedException, ExecutionException {
        LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
        ExecutorService executorService = new ThreadPoolExecutor(threadLimit, threadLimit, 0L, TimeUnit.MILLISECONDS, workQueue);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        final AtomicReference<OperationResult> creationStats = new AtomicReference<>(new OperationResult("creation", 0, 0, 0));
        final AtomicReference<OperationResult> updateStats = new AtomicReference<>(new OperationResult("update", 0, 0, 0));
        final AtomicReference<OperationResult> deletionStats = new AtomicReference<>(new OperationResult("deletion", 0, 0, 0));
        for (int i = 0; i < iterationLimit; i++) {
            while (workQueue.remainingCapacity() == 0) {
                Thread.sleep(100);
            }
            final int iteration = i;
            int operation = RandomUtils.nextInt(0, 3);
            switch (operation) {
                case 0:
                    futures.add(CompletableFuture.supplyAsync(createVertex(maxVertexLimit), executorService)
                                                 .thenAccept(delta -> {
                                                     creationStats.getAndUpdate(s -> s.update(delta));
                                                     if (delta.id != null) {
                                                         operationLog.computeIfAbsent(delta.id, k -> new ConcurrentHashMap<>())
                                                                     .put(iteration, 0);
                                                     }
                                                 }));
                    break;
                case 1:
                    futures.add(CompletableFuture.supplyAsync(updateVertex(), executorService)
                                                 .thenAccept(delta -> {
                                                     updateStats.getAndUpdate(s -> s.update(delta));
                                                     if (delta.id != null && delta.fail != 0) {
                                                         operationLog.computeIfAbsent(delta.id, k -> new ConcurrentHashMap<>())
                                                                     .put(iteration, -1);
                                                     } else if (delta.id != null) {
                                                         operationLog.computeIfAbsent(delta.id, k -> new ConcurrentHashMap<>())
                                                                     .put(iteration, 1);
                                                     }
                                                 }));
                    break;
                case 2:
                    futures.add(CompletableFuture.supplyAsync(deleteVertex(minVertexLimit), executorService)
                                                 .thenAccept(delta -> {
                                                     deletionStats.getAndUpdate(s -> s.update(delta));
                                                     if (delta.id != null && delta.fail != 0) {
                                                         operationLog.computeIfAbsent(delta.id, k -> new ConcurrentHashMap<>())
                                                                     .put(iteration, -2);
                                                     } else if (delta.id != null) {
                                                         operationLog.computeIfAbsent(delta.id, k -> new ConcurrentHashMap<>())
                                                                     .put(iteration, 2);
                                                     }
                                                 }));
                    break;
                default:
                    break;
            }
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[] { })).get();
        return new StringBuilder().append(creationStats).append('\n')
                                  .append(updateStats).append('\n')
                                  .append(deletionStats)
                                  .toString();
    }

    private Supplier<OperationResult> createVertex(long vertexLimit) {
        return () -> {
            if (graph.traversal().V().count().next() >= vertexLimit) {
                return OperationResult.ignored();
            }
            Object id = graph.traversal().addV(VERTEX_LABEL).property(PROPERTY_NAME, RandomStringUtils.randomAlphabetic(4, 20)).id().next();
            graph.traversal().tx().commit();
            return OperationResult.success(id);
        };
    }

    private Supplier<OperationResult> deleteVertex(long vertexLimit) {
        return () -> {
            List<Object> ids = graph.traversal().V().id().toList();
            if (ids.size() <= vertexLimit) {
                return OperationResult.ignored();
            }
            int index = RandomUtils.nextInt(0, ids.size());
            try {
                Object id = ids.get(index);
                return executeWithRetry(() -> {
                    graph.traversal().V(id).drop().iterate();
                    graph.traversal().tx().commit();
                }, 5) ? OperationResult.success(id) : OperationResult.fail(id);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private Supplier<OperationResult> updateVertex() {
        return () -> {
            List<Object> ids = graph.traversal().V().id().toList();
            if (ids.size() == 0) {
                return OperationResult.ignored();
            }
            int index = RandomUtils.nextInt(0, ids.size());
            try {
                Object id = ids.get(index);
                return executeWithRetry(() -> {
                    graph.traversal().V(id).property(PROPERTY_NAME, RandomStringUtils.randomAlphabetic(4, 20)).id().iterate();
                    graph.traversal().tx().commit();
                }, 5) ? OperationResult.success(id) : OperationResult.fail(id);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private boolean executeWithRetry(Runnable runnable, int attemptLimit) throws InterruptedException {
        boolean lockContention;
        boolean success = false;
        int attempts = 0;
        do {
            lockContention = false;
            try {
                runnable.run();
                success = true;
            } catch (JanusGraphException e) {
                if (e.isCausedBy(PermanentBackendException.class)) {
                    lockContention = true;
                } else {
                    throw e;
                }
            }
            Thread.sleep(100);
            attempts++;
        } while (lockContention && attempts < attemptLimit);
        return success;
    }

    @Test
    void consistencyTest() throws Exception {
        String populateStat = populate(100, 10, 50, 5);
        Map<Object, Object> stored = graph.traversal().V().project("id", PROPERTY_NAME).by(__.id()).by(PROPERTY_NAME).toStream()
                                          .collect(Collectors.toMap(m -> m.get("id"), m -> m.get(PROPERTY_NAME)));
        graph.close();

        LuceneIndex luceneIndex = new LuceneIndex(config.restrictTo(BACKING_INDEX));
        RawQuery rawQuery = new RawQuery(VERTEX_LABEL, "*:*", new Parameter[0]);
        final BaseTransactionConfig txConfig = StandardBaseTransactionConfig.of("sys", TimestampProviders.MILLI);
        BaseTransactionConfigurable tx = luceneIndex.beginTransaction(txConfig);
        Map<Long, String> indexed = luceneIndex.queryForDocument(rawQuery, getIndexRetriever(getMapping()), tx)
                                               .collect(
                                                   Collectors.toMap(m -> LongEncoding.decode(m.get(LuceneIndex.DOCID)), m -> m.get(PROPERTY_NAME)));
        tx.commit();
        stored.forEach((k, v) -> {
            Assertions.assertTrue(indexed.containsKey(k), "index does not contain key " + k + "\n"
                                                          + "operations: " + operationLog.get(k) + statMessage(populateStat, stored, indexed));
            Assertions.assertEquals(((String) v).toLowerCase(), indexed.get(k),
                "index contain key " + k + " with different value " + indexed.get(k) + " stored value " + v + "\n"
                + "operations: " + operationLog.get(k) + statMessage(populateStat, stored, indexed));
        });
        indexed.forEach((k, v) -> {
            Assertions.assertTrue(stored.containsKey(k), "store does not contain key " + k + "\n"
                                                         + "operations: " + operationLog.get(k) + statMessage(populateStat, stored, indexed));
            Assertions.assertEquals(v, ((String) stored.get(k)).toLowerCase(),
                "store contain key " + k + " with different value " + stored.get(k) + " indexed value " + v + "\n"
                + "operations: " + operationLog.get(k) + statMessage(populateStat, stored, indexed));
        });
        Assertions.assertEquals(stored.size(), indexed.size(), statMessage(populateStat, stored, indexed));
    }

    private String statMessage(String populateStat, Map<Object, Object> stored, Map<Long, String> indexed) {
        final StringBuffer sb = new StringBuffer();
        sb.append("\n\n");
        sb.append("populate stat:").append('\n');
        sb.append(populateStat).append('\n');
        sb.append('\n');
        sb.append("run stat:").append('\n');
        sb.append("stored vertex number = ").append(stored.size()).append('\n');
        sb.append(stored).append('\n');
        sb.append("indexed vertex number = ").append(indexed.size()).append('\n');
        sb.append(indexed).append('\n');
        sb.append("operation log (vertexId={operation_number=[0-creation, 1-update, 2-deletion]})").append('\n');
        sb.append(operationLog).append('\n');
        return sb.toString();
    }

    public static KeyInformation.IndexRetriever getIndexRetriever(final Map<String, KeyInformation> mappings) {
        return new KeyInformation.IndexRetriever() {

            @Override
            public KeyInformation get(String store, String key) {
                return mappings.get(key);
            }

            @Override
            public KeyInformation.StoreRetriever get(String store) {
                return mappings::get;
            }

            @Override
            public void invalidate(String store) {
                mappings.remove(store);
            }
        };
    }

    public static Map<String, KeyInformation> getMapping() {
        return new HashMap<String, KeyInformation>() {{
            put(PROPERTY_NAME, new StandardKeyInformation(String.class, Cardinality.SINGLE));
        }};
    }
}

class OperationResult {
    public String name = "";
    public int success;
    public int fail;
    public int ignored;
    public Object id;
    public int iteration;

    OperationResult(int success, int fail, int ignored) {
        this.success = success;
        this.fail = fail;
        this.ignored = ignored;
    }

    OperationResult(int success, int fail, int ignored, Object id) {
        this.success = success;
        this.fail = fail;
        this.ignored = ignored;
        this.id = id;
    }

    OperationResult(String name, int success, int fail, int ignored) {
        this(success, fail, ignored);
        this.name = name;
    }

    static OperationResult success(Object id) {
        return new OperationResult(1, 0, 0, id);
    }

    static OperationResult fail(Object id) {
        return new OperationResult(0, 1, 0, id);
    }

    static OperationResult ignored() {
        return new OperationResult(0, 0, 1);
    }

    OperationResult update(OperationResult delta) {
        success += delta.success;
        fail += delta.fail;
        ignored += delta.ignored;
        return this;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(name).append(" success = ").append(success).append('\n');
        sb.append(name).append(" fail = ").append(fail).append('\n');
        sb.append(name).append(" ignored = ").append(ignored);
        return sb.toString();
    }
}
