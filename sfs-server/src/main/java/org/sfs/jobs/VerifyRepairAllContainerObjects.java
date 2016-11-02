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

import io.vertx.core.MultiMap;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.ListSfsObjectIndexes;
import org.sfs.elasticsearch.ScanAndScrollStreamProducer;
import org.sfs.elasticsearch.SearchHitMaintainObjectEndableWrite;
import org.sfs.io.AsyncIO;
import org.sfs.rx.Defer;
import rx.Observable;

import java.util.Calendar;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.sfs.util.DateFormatter.toDateTimeString;

public class VerifyRepairAllContainerObjects extends AbstractJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(VerifyRepairAllContainerObjects.class);
    private ScanAndScrollStreamProducer producer;
    private boolean aborted = false;
    public static final long CONSISTENCY_THRESHOLD = TimeUnit.MINUTES.toMillis(5);
    public static final int VERIFY_RETRY_COUNT = 3;


    @Override
    public String id() {
        return Jobs.ID.VERIFY_REPAIR_ALL_CONTAINERS_OBJECTS;
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

        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        return Defer.aVoid()
                .flatMap(new ListSfsObjectIndexes(vertxContext))
                .toList()
                .flatMap(indeces -> {
                    long now = System.currentTimeMillis() - CONSISTENCY_THRESHOLD;
                    Calendar consistencyThreshold = Calendar.getInstance();
                    consistencyThreshold.setTimeInMillis(now);

                    RangeQueryBuilder query = rangeQuery("update_ts").lte(toDateTimeString(consistencyThreshold));

                    producer =
                            new ScanAndScrollStreamProducer(vertxContext, query)
                                    .setIndeces(indeces.toArray(new String[indeces.size()]))
                                    .setTypes(elasticSearch.defaultType())
                                    .setReturnVersion(true);

                    if (aborted) {
                        producer.abort();
                    }

                    SearchHitMaintainObjectEndableWrite consumer = new SearchHitMaintainObjectEndableWrite(vertxContext);

                    LOGGER.info("Starting maintain");

                    return AsyncIO.pump(producer, consumer)
                            .doOnNext(aVoid1 -> LOGGER.info("Finished maintain"));
                });
    }
}

