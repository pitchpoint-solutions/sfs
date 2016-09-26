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

package org.sfs.elasticsearch.container;

import io.vertx.core.logging.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.global.GlobalBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.vo.ContainerStats;
import org.sfs.vo.PersistentContainer;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.collect.FluentIterable.from;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.nested;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.sfs.util.ExceptionHelper.containsException;
import static rx.Observable.error;
import static rx.Observable.just;

public class LoadContainerStats implements Func1<PersistentContainer, Observable<ContainerStats>> {

    private static final Logger LOGGER = getLogger(LoadContainerStats.class);
    private final VertxContext<Server> vertxContext;

    public LoadContainerStats(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<ContainerStats> call(PersistentContainer persistentContainer) {
        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();
        String containerId = persistentContainer.getId();

        GlobalBuilder aggregation =
                global("all_objects")
                        .subAggregation(
                                filter("filter_by_container")
                                        .filter(termQuery("container_id", containerId))
                                        .subAggregation(
                                                nested("nested_segments")
                                                        .path("versions.segments")
                                                        .subAggregation(
                                                                sum("bytes_used")
                                                                        .field("versions.segments.read_length")
                                                        )
                                        )
                        );

        String objectIndex = elasticSearch.objectIndex(persistentContainer.getName());

        SearchRequestBuilder aggregateRequest = elasticSearch.get()
                .prepareSearch(objectIndex)
                .setTypes(elasticSearch.defaultType())
                .setSize(0)
                .addAggregation(aggregation)
                .setTimeout(timeValueMillis(elasticSearch.getDefaultSearchTimeout() - 10));


        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Search Request = %s", Jsonify.toString(aggregateRequest)));
        }

        return elasticSearch.execute(vertxContext, aggregateRequest, elasticSearch.getDefaultSearchTimeout())
                .map(oSearchResponse -> {

                    SearchResponse searchResponse = oSearchResponse.get();

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(format("Search Response = %s", Jsonify.toString(searchResponse)));
                    }

                    long objectCount = 0;
                    double bytesUsed = 0;
                    for (SingleBucketAggregation globalAgg : toSingleBucket(searchResponse.getAggregations())) {
                        if ("all_objects".equals(globalAgg.getName())) {
                            for (SingleBucketAggregation filterAgg : toSingleBucket(globalAgg.getAggregations())) {
                                if ("filter_by_container".equals(filterAgg.getName())) {
                                    objectCount += filterAgg.getDocCount();
                                    for (SingleBucketAggregation nestAgg : toSingleBucket(filterAgg.getAggregations())) {
                                        if ("nested_segments".equals(nestAgg.getName())) {
                                            for (Aggregation bytesUsedAgg : nestAgg.getAggregations()) {
                                                if ("bytes_used".equals(bytesUsedAgg.getName())) {
                                                    Sum sum = (Sum) bytesUsedAgg;
                                                    bytesUsed += sum.getValue();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    return new ContainerStats(bytesUsed, objectCount, persistentContainer);
                })
                .onErrorResumeNext(throwable -> {
                    if (containsException(IndexNotFoundException.class, throwable)) {
                        return just(new ContainerStats(0, 0, persistentContainer));
                    } else {
                        return error(throwable);
                    }
                });
    }


    protected Iterable<SingleBucketAggregation> toSingleBucket(Aggregations aggregations) {
        return from(aggregations)
                .transform(input -> (SingleBucketAggregation) input);
    }
}
