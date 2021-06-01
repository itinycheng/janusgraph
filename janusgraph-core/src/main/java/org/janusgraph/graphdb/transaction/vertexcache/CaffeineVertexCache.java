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

package org.janusgraph.graphdb.transaction.vertexcache;

import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.util.datastructures.Retriever;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CaffeineVertexCache implements VertexCache {
    private static final Logger log = LoggerFactory.getLogger(CaffeineVertexCache.class);
    private final Cache<Long, InternalVertex> cache;

    public CaffeineVertexCache(final long maxCacheSize) {
        cache = Caffeine.newBuilder().maximumWeight(maxCacheSize).weigher((k, v) -> {
            InternalVertex vertex = (InternalVertex) v;
            if (vertex.isNew() || vertex.isModified() || vertex.isRemoved()) {
                return 0;
            }
            return 1;
        }).build();
        log.debug("Created vertex cache with max size {}", maxCacheSize);
    }

    @Override
    public boolean contains(long vertexId) {
        return cache.asMap().containsKey(vertexId);
    }

    @Override
    public InternalVertex get(final long vertexId, final Retriever<Long, InternalVertex> retriever) {
        return cache.get(vertexId, retriever::get);
    }

    @Override
    public void add(InternalVertex vertex, long vertexId) {
        Preconditions.checkNotNull(vertex);
        Preconditions.checkArgument(vertexId != 0);
        cache.put(vertexId, vertex);
    }

    @Override
    public List<InternalVertex> getAllNew() {
        final List<InternalVertex> vertices = new ArrayList<>(10);
        for (InternalVertex v : cache.asMap().values()) {
            if (v.isNew()) {
                vertices.add(v);
            }
        }
        return vertices;
    }

    @Override
    public synchronized void close() {
        cache.invalidateAll();
        cache.cleanUp();
    }
}
