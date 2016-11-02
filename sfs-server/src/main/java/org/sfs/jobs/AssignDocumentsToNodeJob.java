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

package org.sfs.jobs;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import io.vertx.core.MultiMap;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.ListSfsStorageIndexes;
import org.sfs.elasticsearch.ScanAndScrollStreamProducer;
import org.sfs.elasticsearch.SearchHitEndableWriteStreamUpdateNodeId;
import org.sfs.io.AsyncIO;
import org.sfs.nodes.ClusterInfo;
import org.sfs.rx.Defer;
import org.sfs.rx.ToVoid;
import rx.Observable;

import java.util.HashMap;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

public class AssignDocumentsToNodeJob extends AbstractJob {

    private ScanAndScrollStreamProducer producer;
    private boolean aborted = false;

    @Override
    public String id() {
        return Jobs.ID.ASSIGN_DOCUMENTS_TO_NODE;
    }

    @Override
    public Observable<Void> executeImpl(VertxContext<Server> vertxContext, MultiMap parameters) {
        return execute0(vertxContext);
    }

    @Override
    public Observable<Void> stopImpl(VertxContext<Server> vertxContext) {
        aborted = true;
        if (producer != null) {
            producer.abort();
        }
        return Defer.aVoid();
    }


    protected Observable<Void> execute0(VertxContext<Server> vertxContext) {
        return Defer.aVoid()
                .flatMap(aVoid -> {
                    ClusterInfo clusterInfo = vertxContext.verticle().getClusterInfo();
                    return Observable.from(clusterInfo.getDataNodes());
                })
                .reduce(new HashMap<String, Long>(), (documentCountsByNode, persistentServiceDef) -> {
                    Optional<Long> documentCount = persistentServiceDef.getDocumentCount();
                    if (documentCount.isPresent()) {
                        documentCountsByNode.put(persistentServiceDef.getId(), documentCount.get());
                    }
                    return documentCountsByNode;
                })
                .filter(stringLongHashMap -> !stringLongHashMap.isEmpty())
                .flatMap(documentCountsByNode -> {
                    Elasticsearch elasticsearch = vertxContext.verticle().elasticsearch();

                    String[] activeNodeIds = Iterables.toArray(documentCountsByNode.keySet(), String.class);

                    BoolQueryBuilder c0 = boolQuery()
                            .mustNot(existsQuery("node_id"));

                    BoolQueryBuilder c1 = boolQuery()
                            .mustNot(termsQuery("node_id", activeNodeIds));

                    BoolQueryBuilder query = boolQuery()
                            .should(c0)
                            .should(c1)
                            .minimumNumberShouldMatch(1);

                    return Defer.aVoid()
                            .flatMap(new ListSfsStorageIndexes(vertxContext))
                            .flatMap(index -> {
                                producer =
                                        new ScanAndScrollStreamProducer(vertxContext, query)
                                                .setIndeces(index)
                                                .setTypes(elasticsearch.defaultType())
                                                .setReturnVersion(true);

                                SearchHitEndableWriteStreamUpdateNodeId consumer = new SearchHitEndableWriteStreamUpdateNodeId(vertxContext, documentCountsByNode);

                                if (aborted) {
                                    producer.abort();
                                }

                                return AsyncIO.pump(producer, consumer);
                            })
                            .count()
                            .map(new ToVoid<>());
                })
                .singleOrDefault(null);
    }
}
