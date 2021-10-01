// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.database.cache;

import com.codahale.metrics.MetricRegistry;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.graphdb.types.system.BaseRelationType;
import org.janusgraph.util.stats.MetricManager;

import static org.janusgraph.diskstorage.util.CacheMetricsAction.MISS;
import static org.janusgraph.diskstorage.util.CacheMetricsAction.RETRIEVAL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class MetricInstrumentedSchemaCache implements SchemaCache {

    public static final String METRICS_NAME = "schemacache";

    public static final String METRICS_TYPENAME = "name";
    public static final String METRICS_RELATIONS = "relations";

    private final SchemaCache cache;

    private final String typeMiss;
    private final String typeRetrieval;
    private final String relationMiss;
    private final String relationRetrieval;

    public MetricInstrumentedSchemaCache(final String metricsPrefix, final StoreRetrieval retriever) {
        String sysMetricsPrefix = metricsPrefix + ".sys";
        typeMiss = MetricRegistry.name(sysMetricsPrefix, METRICS_NAME, METRICS_TYPENAME, MISS.getName());
        typeRetrieval = MetricRegistry.name(sysMetricsPrefix, METRICS_NAME, METRICS_TYPENAME, RETRIEVAL.getName());
        relationMiss = MetricRegistry.name(sysMetricsPrefix, METRICS_NAME, METRICS_RELATIONS, MISS.getName());
        relationRetrieval = MetricRegistry.name(sysMetricsPrefix, METRICS_NAME, METRICS_RELATIONS, RETRIEVAL.getName());

        cache = new StandardSchemaCache(new StoreRetrieval() {

            @Override
            public Long retrieveSchemaByName(String typeName) {
                MetricManager.INSTANCE.getCounter(typeMiss).inc();
                return retriever.retrieveSchemaByName(typeName);
            }

            @Override
            public EntryList retrieveSchemaRelations(long schemaId, BaseRelationType type, Direction dir) {
                MetricManager.INSTANCE.getCounter(relationMiss).inc();
                return retriever.retrieveSchemaRelations(schemaId, type, dir);
            }
        });
    }

    @Override
    public Long getSchemaId(String schemaName) {
        MetricManager.INSTANCE.getCounter(typeRetrieval).inc();
        return cache.getSchemaId(schemaName);
    }

    @Override
    public EntryList getSchemaRelations(long schemaId, BaseRelationType type, Direction dir) {
        MetricManager.INSTANCE.getCounter(relationRetrieval).inc();
        return cache.getSchemaRelations(schemaId, type, dir);
    }

    @Override
    public void expireSchemaElement(long schemaId) {
        cache.expireSchemaElement(schemaId);
    }

}
