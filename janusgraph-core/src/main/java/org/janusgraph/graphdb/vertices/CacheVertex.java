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

package org.janusgraph.graphdb.vertices;

import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.util.datastructures.Retriever;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TX_VERTEX_CACHE_TYPE;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class CacheVertex extends StandardVertex {
    public static final NoQueryCache NO_QUERY_CACHE = new NoQueryCache();

    // We don't try to be smart and match with previous queries
    // because that would waste more cycles on lookup than save actual memory
    // We use a normal map with synchronization since the likelihood of contention
    // is super low in a single transaction
    protected final QueryCache queryCache;

    public CacheVertex(StandardJanusGraphTx tx, Object id, byte lifecycle) {
        super(tx, id, lifecycle);
        // if (!tx.getGraph().getBackend().getStoreFeatures().isDistributed()) {
        //     queryCache = new NoQueryCache();
        // } else {
        final String cacheType = tx.getConfiguration().getCustomOption(TX_VERTEX_CACHE_TYPE);
        switch (cacheType) {
            case "DEFAULT":
                queryCache = new HashMapQueryCache();
                break;
            case "TREEMAP":
                queryCache = new TreeMapQueryCache();
                break;
            case "NOCACHE":
                queryCache = NO_QUERY_CACHE;
                break;
            default:
                throw new RuntimeException("invalid type " + cacheType);
                // }
        }
    }

    public void refresh() {
        synchronized (queryCache) {
            queryCache.clear();
        }
    }

    protected void addToQueryCache(final SliceQuery query, final EntryList entries) {
        synchronized (queryCache) {
            //TODO: become smarter about what to cache and when (e.g. memory pressure)
            queryCache.put(query, entries);
        }
    }

    protected int getQueryCacheSize() {
        synchronized (queryCache) {
            return queryCache.size();
        }
    }

    @Override
    public EntryList loadRelations(final SliceQuery query, final Retriever<SliceQuery, EntryList> lookup) {
        if (isNew()) {
            return EntryList.EMPTY_LIST;
        }

        EntryList result;
        synchronized (queryCache) {
            result = queryCache.get(query);
        }
        if (result == null) {
            //First check for super
            Map.Entry<SliceQuery, EntryList> superset = getSuperResultSet(query);
            if (superset == null || superset.getValue() == null) {
                result = lookup.get(query);
            } else {
                result = query.getSubset(superset.getKey(), superset.getValue());
            }
            addToQueryCache(query, result);
        }
        return result;
    }

    @Override
    public boolean hasLoadedRelations(final SliceQuery query) {
        synchronized (queryCache) {
            return queryCache.get(query) != null || getSuperResultSet(query) != null;
        }
    }

    private Map.Entry<SliceQuery, EntryList> getSuperResultSet(final SliceQuery query) {

        synchronized (queryCache) {
            if (queryCache.size() > 0) {
                return queryCache.getSuperResultSet(query);
            }
        }
        return null;
    }

    interface QueryCache {
        Map.Entry<SliceQuery, EntryList> getSuperResultSet(SliceQuery query);

        EntryList get(SliceQuery query);

        EntryList put(SliceQuery query, EntryList entries);

        int size();

        void clear();
    }

    static class NoQueryCache implements QueryCache {

        @Override
        public Map.Entry<SliceQuery, EntryList> getSuperResultSet(final SliceQuery query) {
            return null;
        }

        public EntryList get(final SliceQuery query) {
            return null;
        }

        @Override
        public EntryList put(final SliceQuery query, final EntryList entries) {
            return entries;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public void clear() {
        }
    }

    static class HashMapQueryCache extends HashMap<SliceQuery, EntryList> implements QueryCache {

        @Override
        public Map.Entry<SliceQuery, EntryList> getSuperResultSet(final SliceQuery query) {
            for (Map.Entry<SliceQuery, EntryList> entry : entrySet()) {
                if (entry.getKey().subsumes(query)) {
                    return entry;
                }
            }
            return null;
        }

        @Override
        public EntryList get(final SliceQuery query) {
            return super.get(query);
        }
    }

    static class SliceQueryComparator implements Comparator<SliceQuery> {

        @Override
        public int compare(final SliceQuery o1, final SliceQuery o2) {
            final int start = o1.getSliceStart().compareTo(o2.getSliceStart());
            if (start == 0) {
                final int end = -1 * o1.getSliceEnd().compareTo(o2.getSliceEnd());
                if (end == 0) {
                    return Integer.compare(o1.getLimit(), o2.getLimit());
                }
                return end;
            } else {
                return start;
            }
        }
    }

    static class TreeMapQueryCache extends TreeMap<SliceQuery, EntryList> implements QueryCache {
        TreeMapQueryCache() {
            super(new SliceQueryComparator());
        }

        @Override
        public Map.Entry<SliceQuery, EntryList> getSuperResultSet(final SliceQuery query) {
            for (Map.Entry<SliceQuery, EntryList> entry : headMap(query, true).entrySet()) {
                if (entry.getKey().subsumes(query)) {
                    return entry;
                }
            }
            return null;
        }

        @Override
        public EntryList get(final SliceQuery query) {
            return super.get(query);
        }
    }
}
