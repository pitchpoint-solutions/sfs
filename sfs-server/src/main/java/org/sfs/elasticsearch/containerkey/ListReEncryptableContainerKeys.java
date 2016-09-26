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

package org.sfs.elasticsearch.containerkey;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import io.vertx.core.json.JsonObject;
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
import org.sfs.elasticsearch.account.LoadAccount;
import org.sfs.elasticsearch.container.LoadContainer;
import org.sfs.vo.PersistentAccount;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.PersistentContainerKey;
import rx.Observable;
import rx.functions.Func1;

import java.util.Calendar;

import static com.google.common.cache.CacheBuilder.newBuilder;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.sfs.rx.Defer.just;
import static org.sfs.util.DateFormatter.toDateTimeString;
import static org.sfs.vo.PersistentContainerKey.fromSearchHit;
import static rx.Observable.from;

public class ListReEncryptableContainerKeys implements Func1<Void, Observable<PersistentContainerKey>> {

    private static final Logger LOGGER = getLogger(ListReEncryptableContainerKeys.class);
    private final VertxContext<Server> vertxContext;
    private final Calendar threshold;
    private final String nodeId;

    public ListReEncryptableContainerKeys(VertxContext<Server> vertxContext, String nodeId, Calendar threshold) {
        this.vertxContext = vertxContext;
        this.threshold = threshold;
        this.nodeId = nodeId;
    }

    @Override
    public Observable<PersistentContainerKey> call(Void aVoid) {

        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        RangeQueryBuilder reEncryptThresholdExceededFilter =
                rangeQuery("re_encrypt_ts")
                        .lte(toDateTimeString(threshold));

        TermQueryBuilder nodeFilter = termQuery("node_id", nodeId);

        BoolQueryBuilder query = boolQuery().must(nodeFilter).must(reEncryptThresholdExceededFilter);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Search Request {%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.containerKeyIndex(), Jsonify.toString(query)));
        }

        SearchRequestBuilder request =
                elasticSearch.get()
                        .prepareSearch(
                                elasticSearch.containerKeyIndex())
                        .setVersion(true)
                        .setTypes(elasticSearch.defaultType())
                        .setQuery(query)
                        .setVersion(true)
                        .setSize(5000)
                        .setTimeout(timeValueMillis(elasticSearch.getDefaultSearchTimeout() - 10));

        Cache<String, PersistentAccount> accountCache = newBuilder().maximumSize(1000).build();
        Cache<String, PersistentContainer> containerCache = newBuilder().maximumSize(1000).build();

        return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultSearchTimeout())
                .flatMap(oSearchResponse -> {
                    SearchResponse searchResponse = oSearchResponse.get();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(format("Search Response {%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.containerKeyIndex(), Jsonify.toString(searchResponse)));
                    }
                    if (oSearchResponse.isPresent()) {
                        return from(oSearchResponse.get().getHits());
                    } else {
                        return from(emptyList());
                    }
                })
                .flatMap(searchHit -> {
                    JsonObject jsonObject = new JsonObject(searchHit.getSourceAsString());
                    String accountId = jsonObject.getString("account_id");
                    String containerId = jsonObject.getString("container_id");
                    return loadAccount(vertxContext, accountId, accountCache)
                            .flatMap(persistentAccount -> loadContainer(vertxContext, persistentAccount, containerId, containerCache))
                            .map(persistentContainer -> fromSearchHit(persistentContainer, searchHit));
                });
    }

    protected Observable<PersistentAccount> loadAccount(VertxContext<Server> vertxContext, String accountId, Cache<String, PersistentAccount> accountCache) {
        PersistentAccount persistentAccount = accountCache.getIfPresent(accountId);
        if (persistentAccount != null) {
            return just(persistentAccount);
        } else {
            return just(accountId)
                    .flatMap(new LoadAccount(vertxContext))
                    .map(Optional::get)
                    .map(persistentAccount1 -> {
                        accountCache.put(accountId, persistentAccount1);
                        return persistentAccount1;
                    });
        }
    }

    protected Observable<PersistentContainer> loadContainer(VertxContext<Server> vertxContext, PersistentAccount persistentAccount, String containerId, Cache<String, PersistentContainer> containerCache) {
        PersistentContainer persistentContainer = containerCache.getIfPresent(containerId);
        if (persistentContainer != null) {
            return just(persistentContainer);
        } else {
            return just(containerId)
                    .flatMap(new LoadContainer(vertxContext, persistentAccount))
                    .map(Optional::get)
                    .map(persistentContainer1 -> {
                        containerCache.put(containerId, persistentContainer1);
                        return persistentContainer1;
                    });
        }
    }
}