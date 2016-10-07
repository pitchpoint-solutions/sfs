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

package org.sfs.elasticsearch.masterkey;

import io.vertx.core.logging.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.vo.PersistentMasterKey;
import rx.Observable;
import rx.functions.Func1;

import java.util.Calendar;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.sfs.util.DateFormatter.toDateTimeString;
import static rx.Observable.from;

public class ListReEncryptableMasterKeys implements Func1<Void, Observable<PersistentMasterKey>> {

    private static final Logger LOGGER = getLogger(ListReEncryptableMasterKeys.class);
    private final VertxContext<Server> vertxContext;
    private final Calendar threshold;
    private final String nodeId;

    public ListReEncryptableMasterKeys(VertxContext<Server> vertxContext, String nodeId, Calendar threshold) {
        this.vertxContext = vertxContext;
        this.threshold = threshold;
        this.nodeId = nodeId;
    }

    @Override
    public Observable<PersistentMasterKey> call(Void aVoid) {

        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        RangeQueryBuilder thresholdFilter =
                rangeQuery("re_encrypt_ts")
                        .lte(toDateTimeString(threshold));

        TermQueryBuilder nodeFilter = termQuery("node_id", nodeId);

        BoolQueryBuilder query = boolQuery().must(nodeFilter).must(thresholdFilter);


        SearchRequestBuilder request =
                elasticSearch.get()
                        .prepareSearch(
                                elasticSearch.masterKeyTypeIndex())
                        .setVersion(true)
                        .setTypes(elasticSearch.defaultType())
                        .setQuery(query)
                        .setSize(5000)
                        .setTimeout(timeValueMillis(elasticSearch.getDefaultSearchTimeout() - 10));

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Search Request {%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.masterKeyTypeIndex(), Jsonify.toString(request)));
        }

        return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultSearchTimeout())
                .flatMap(oSearchResponse -> {
                    SearchResponse searchResponse = oSearchResponse.get();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(format("Search Response {%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.masterKeyTypeIndex(), Jsonify.toString(searchResponse)));
                    }
                    if (oSearchResponse.isPresent()) {
                        return from(oSearchResponse.get().getHits());
                    } else {
                        return from(emptyList());
                    }
                })
                .map(PersistentMasterKey::fromSearchHit);
    }
}
