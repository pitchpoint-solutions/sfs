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
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.ListSfsObjectIndexes;
import org.sfs.elasticsearch.ScanAndScrollStreamProducer;
import org.sfs.elasticsearch.SearchHitMaintainObjectEndableWrite;
import org.sfs.rx.ToVoid;
import rx.Observable;
import rx.functions.Func1;

import java.util.Calendar;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.util.Calendar.MILLISECOND;
import static java.util.Calendar.getInstance;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.sfs.io.AsyncIO.pump;
import static org.sfs.rx.Defer.empty;
import static org.sfs.util.DateFormatter.toDateTimeString;

public class MaintainObjectsForNode implements Func1<Void, Observable<Void>> {

    private static final Logger LOGGER = getLogger(MaintainObjectsForNode.class);
    private final VertxContext<Server> vertxContext;
    public static final int CONSISTENCY_THRESHOLD = 86400000;
    public static final int VERIFY_RETRY_COUNT = 3;
    private String nodeId;

    public MaintainObjectsForNode(VertxContext<Server> vertxContext, String nodeId) {
        this.vertxContext = vertxContext;
        this.nodeId = nodeId;
    }

    @Override
    public Observable<Void> call(final Void aVoid) {
        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        return empty()
                .flatMap(new ListSfsObjectIndexes(vertxContext))
                .flatMap(index -> {
                    Calendar consistencyThreshold = getInstance();
                    consistencyThreshold.add(MILLISECOND, -CONSISTENCY_THRESHOLD);

                    RangeQueryBuilder query = rangeQuery("update_ts").lte(toDateTimeString(consistencyThreshold));

                    ScanAndScrollStreamProducer producer =
                            new ScanAndScrollStreamProducer(vertxContext, query)
                                    .setIndeces(index)
                                    .setTypes(elasticSearch.defaultType())
                                    .setReturnVersion(true);

                    SearchHitMaintainObjectEndableWrite consumer = new SearchHitMaintainObjectEndableWrite(vertxContext);

                    LOGGER.info("Starting " + index);

                    return pump(producer, consumer)
                            .doOnNext(aVoid1 -> LOGGER.info("Finished " + index));
                })
                .count()
                .map(new ToVoid<>());

    }
}
