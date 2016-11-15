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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
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
import org.sfs.rx.ToVoid;
import rx.Observable;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.sfs.util.DateFormatter.toDateTimeString;

public class VerifyRepairAllContainerObjects extends AbstractJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(VerifyRepairAllContainerObjects.class);
    private boolean aborted = false;
    public static final long CONSISTENCY_THRESHOLD = TimeUnit.MINUTES.toMillis(5);
    public static final int VERIFY_RETRY_COUNT = 3;
    private List<ScanAndScrollStreamProducer> producerList = new ArrayList<>();


    @Override
    public String id() {
        return Jobs.ID.VERIFY_REPAIR_ALL_CONTAINERS_OBJECTS;
    }

    @Override
    public Observable<Void> executeImpl(VertxContext<Server> vertxContext, MultiMap parameters) {
        return execute0(vertxContext, parameters);
    }

    @Override
    public Observable<Void> stopImpl(VertxContext<Server> vertxContext) {
        aborted = true;
        for (ScanAndScrollStreamProducer p : producerList) {
            p.abort();
        }
        return Defer.aVoid();
    }


    protected Observable<Void> execute0(VertxContext<Server> vertxContext, MultiMap parameters) {

        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        return Defer.aVoid()
                .flatMap(new ListSfsObjectIndexes(vertxContext))
                .flatMap(index -> {

                    String unparsedForceRemoveVolumes = JobParams.getFirstOptionalParam(parameters, Jobs.Parameters.FORCE_REMOVE_VOLUMES);
                    Set<String> forceRemoveVolumes = Strings.isNullOrEmpty(unparsedForceRemoveVolumes)
                            ? Collections.emptySet()
                            : Sets.newHashSet(Splitter.on(',').omitEmptyStrings().trimResults().split(unparsedForceRemoveVolumes));

                    long now = System.currentTimeMillis() - CONSISTENCY_THRESHOLD;
                    Calendar consistencyThreshold = Calendar.getInstance();
                    consistencyThreshold.setTimeInMillis(now);

                    RangeQueryBuilder query = rangeQuery("update_ts").lte(toDateTimeString(consistencyThreshold));

                    ScanAndScrollStreamProducer producer =
                            new ScanAndScrollStreamProducer(vertxContext, query)
                                    .setIndeces(index)
                                    .setTypes(elasticSearch.defaultType())
                                    .setReturnVersion(true);
                    producerList.add(producer);

                    if (aborted) {
                        producer.abort();
                    }

                    SearchHitMaintainObjectEndableWrite consumer = new SearchHitMaintainObjectEndableWrite(vertxContext, forceRemoveVolumes);

                    LOGGER.info("Starting maintain on index " + index);

                    return AsyncIO.pump(producer, consumer)
                            .doOnNext(aVoid1 -> LOGGER.info("Finished maintain on index " + index));
                })
                .count()
                .map(new ToVoid<>());
    }
}

