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

package org.janusgraph.graphdb.database;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.RelationCategory;

import java.util.EnumMap;
import java.util.Objects;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class RelationQueryCache implements AutoCloseable {

    private final Cache<CacheKey, CacheEntry> cache;
    private final EdgeSerializer edgeSerializer;

    private final EnumMap<RelationCategory,SliceQuery> relationTypes;

    public RelationQueryCache(EdgeSerializer edgeSerializer) {
        this(edgeSerializer,256);
    }

    public RelationQueryCache(EdgeSerializer edgeSerializer, int capacity) {
        this.edgeSerializer = edgeSerializer;
        this.cache = Caffeine.newBuilder().maximumSize(capacity * 3L / 2).initialCapacity(capacity)
                             .build();
        relationTypes = new EnumMap<>(RelationCategory.class);
        for (RelationCategory rt : RelationCategory.values()) {
            relationTypes.put(rt,edgeSerializer.getQuery(rt,false));
        }
    }

    public void expireQueryForType(InternalRelationType type) {
        // invoke from management system
    }

    public SliceQuery getQuery(RelationCategory type) {
        return relationTypes.get(type);
    }

    public SliceQuery getQuery(final InternalRelationType type, Direction dir, int limit) {
        CacheEntry ce = cache.get(new CacheKey(type.longId(), limit), (key) -> new CacheEntry(edgeSerializer, type));
        assert ce!=null;
        Preconditions.checkArgument(type.isUnidirected(Direction.BOTH) || type.isUnidirected(dir), "Type is %s dir %s", type, dir);
        return ce.get(dir);
    }

    public void close() {
        cache.invalidateAll();
        cache.cleanUp();
    }

    private static final class CacheKey {
        private final long keyId;
        private final int limit;

        private CacheKey(final long keyId, final int limit) {
            this.keyId = keyId;
            this.limit = limit;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return keyId == cacheKey.keyId && limit == cacheKey.limit;
        }

        @Override
        public int hashCode() {
            return Objects.hash(keyId, limit);
        }
    }

    private static final class CacheEntry {

        private final SliceQuery in;
        private final SliceQuery out;
        private final SliceQuery both;

        public CacheEntry(EdgeSerializer edgeSerializer, InternalRelationType t) {
            if (t.isPropertyKey()) {
                out = edgeSerializer.getQuery(t, Direction.OUT,new EdgeSerializer.TypedInterval[t.getSortKey().length]);
                in = out;
                both = out;
            } else {
                out = edgeSerializer.getQuery(t,Direction.OUT,
                            new EdgeSerializer.TypedInterval[t.getSortKey().length]);

                if (t.isUnidirected(Direction.BOTH) || t.isUnidirected(Direction.IN)) {
                    in = edgeSerializer.getQuery(t, Direction.IN, new EdgeSerializer.TypedInterval[t.getSortKey().length]);
                    both = edgeSerializer.getQuery(t,Direction.BOTH, new EdgeSerializer.TypedInterval[t.getSortKey().length]);
                } else {
                    in = null;
                    both = null;
                }

            }
        }

        public SliceQuery get(Direction dir) {
            switch (dir) {
                case IN: return in;
                case OUT: return out;
                case BOTH: return both;
                default: throw new AssertionError("Unknown direction: " + dir);
            }
        }

    }

}
