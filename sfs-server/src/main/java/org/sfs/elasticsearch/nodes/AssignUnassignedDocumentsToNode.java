/*
 * Copyright 2016 The Simple File Server Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sfs.elasticsearch.nodes;

import io.vertx.core.logging.Logger;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.ListSfsStorageIndexes;
import org.sfs.elasticsearch.ScanAndScrollStreamProducer;
import org.sfs.elasticsearch.SearchHitEndableWriteStreamUpdateNodeId;
import org.sfs.rx.ToVoid;
import rx.Observable;
import rx.functions.Func1;

import java.util.Map;

import static com.google.common.collect.Iterables.toArray;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.sfs.io.AsyncIO.pump;
import static org.sfs.rx.Defer.empty;

public class AssignUnassignedDocumentsToNode implements Func1<Void, Observable<Void>> {

    private static final Logger LOGGER = getLogger(AssignUnassignedDocumentsToNode.class);
    private final VertxContext<Server> vertxContext;
    private final Map<String, Long> documentCountByNode;

    public AssignUnassignedDocumentsToNode(VertxContext<Server> vertxContext, Map<String, Long> documentCountByNode) {
        this.vertxContext = vertxContext;
        this.documentCountByNode = documentCountByNode;
    }

    @Override
    public Observable<Void> call(Void aVoid) {

        Elasticsearch elasticsearch = vertxContext.verticle().elasticsearch();

        String[] activeNodeIds = toArray(documentCountByNode.keySet(), String.class);

        BoolQueryBuilder c0 = boolQuery()
                .mustNot(existsQuery("node_id"));

        BoolQueryBuilder c1 = boolQuery()
                .mustNot(termsQuery("node_id", activeNodeIds));

        BoolQueryBuilder query = boolQuery()
                .should(c0)
                .should(c1)
                .minimumNumberShouldMatch(1);

        return empty()
                .flatMap(new ListSfsStorageIndexes(vertxContext))
                .flatMap(index -> {
                    ScanAndScrollStreamProducer producer =
                            new ScanAndScrollStreamProducer(vertxContext, query)
                                    .setIndeces(index)
                                    .setTypes(elasticsearch.defaultType())
                                    .setReturnVersion(true);

                    SearchHitEndableWriteStreamUpdateNodeId consumer = new SearchHitEndableWriteStreamUpdateNodeId(vertxContext, documentCountByNode);

                    return pump(producer, consumer);
                })
                .count()
                .map(new ToVoid<>());

    }
}