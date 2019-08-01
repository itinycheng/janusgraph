// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.diskstorage.lucene;

import java.io.*;

import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;

public class SimpleDocumentCollector extends SimpleCollector {
    final IntArrayList docs;
    final DoubleArrayList scores;

    private static final int EXPECTED_ELEMENTS = 10;
    private final boolean needScore;
    private final int limit;
    private int numDocs = 0;
    private int base = 0;
    private Scorable scorer;

    SimpleDocumentCollector(int limit, final boolean needScore) {
        int expectedElements = Math.min(limit, EXPECTED_ELEMENTS);
        this.docs = new IntArrayList(expectedElements);
        this.scores = new DoubleArrayList(expectedElements);
        this.limit = limit;
        this.needScore = needScore;
    }

    public void collect(int doc) throws IOException {
        if (numDocs++ < limit) {
            double score = 0.0;
            if (needScore) {
                score = scorer.score();
            }
            docs.add(this.base + doc);
            scores.add(score);
        }
    }

    @Override
    public void setScorer(final Scorable scorer) {
        this.scorer = scorer;
    }

    @Override
    public ScoreMode scoreMode() {
        return needScore ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    protected void doSetNextReader(LeafReaderContext context) {
        this.base = context.docBase;
    }
}
