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

package org.sfs.elasticsearch.object;

import io.vertx.core.logging.Logger;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.ScanAndScrollStreamProducer;
import org.sfs.elasticsearch.SearchHitMaintainObjectEndableWrite;
import rx.Observable;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.sfs.io.AsyncIO.pump;
import static org.sfs.vo.ObjectPath.fromPaths;

public class MaintainSingleObject implements Func1<Void, Observable<Void>> {

    private static final Logger LOGGER = getLogger(MaintainSingleObject.class);
    private final VertxContext<Server> vertxContext;
    private final String objectId;

    public MaintainSingleObject(VertxContext<Server> vertxContext, String objectId) {
        this.vertxContext = vertxContext;
        this.objectId = objectId;
    }

    @Override
    public Observable<Void> call(final Void aVoid) {
        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();


        String containerName = fromPaths(objectId).containerName().get();

        String objectIndex = elasticSearch.objectIndex(containerName);

        TermQueryBuilder query = termQuery("_id", objectId);

        ScanAndScrollStreamProducer producer =
                new ScanAndScrollStreamProducer(vertxContext, query)
                        .setIndeces(objectIndex)
                        .setTypes(elasticSearch.defaultType())
                        .setReturnVersion(true);

        SearchHitMaintainObjectEndableWrite consumer = new SearchHitMaintainObjectEndableWrite(vertxContext);

        LOGGER.info("Starting");

        return pump(producer, consumer)
                .doOnNext(aVoid1 -> LOGGER.info("Finished"));
    }
}
