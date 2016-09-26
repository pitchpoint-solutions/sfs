/*
 *
 * Copyright (C) 2009 The Simple File Server Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS-IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sfs.elasticsearch.documents;

import io.vertx.core.logging.Logger;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.ScanAndScrollStreamProducer;
import org.sfs.elasticsearch.SearchHitEndableWriteStreamToJsonLine;
import org.sfs.io.BufferEndableWriteStream;
import rx.Observable;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.sfs.io.AsyncIO.pump;

public class DumpDocumentsForNode implements Func1<Void, Observable<Void>> {

    private static final Logger LOGGER = getLogger(DumpDocumentsForNode.class);
    private final VertxContext<Server> vertxContext;
    private final String nodeId;
    private final String index;
    private final BufferEndableWriteStream bufferStreamConsumer;

    public DumpDocumentsForNode(VertxContext<Server> vertxContext, String nodeId, String index, BufferEndableWriteStream bufferStreamConsumer) {
        this.vertxContext = vertxContext;
        this.nodeId = nodeId;
        this.index = index;
        this.bufferStreamConsumer = bufferStreamConsumer;
    }

    @Override
    public Observable<Void> call(Void aVoid) {

        Elasticsearch elasticsearch = vertxContext.verticle().elasticsearch();

        BoolQueryBuilder query = boolQuery()
                .must(existsQuery("node_id"))
                .must(termQuery("node_id", nodeId));

        ScanAndScrollStreamProducer producer =
                new ScanAndScrollStreamProducer(vertxContext, query)
                        .setIndeces(index)
                        .setTypes(elasticsearch.defaultType());

        SearchHitEndableWriteStreamToJsonLine consumer = new SearchHitEndableWriteStreamToJsonLine(bufferStreamConsumer);
        return pump(producer, consumer);
    }
}
