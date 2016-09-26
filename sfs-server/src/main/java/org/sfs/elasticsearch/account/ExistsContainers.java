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

package org.sfs.elasticsearch.account;

import com.google.common.base.Optional;
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.vo.PersistentAccount;
import rx.Observable;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class ExistsContainers implements Func1<PersistentAccount, Observable<Boolean>> {

    private static final Logger LOGGER = getLogger(ExistsContainers.class);
    private final VertxContext<Server> vertxContext;

    public ExistsContainers(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<Boolean> call(PersistentAccount persistentAccount) {
        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        SearchRequestBuilder request = elasticSearch.get()
                .prepareSearch(elasticSearch.containerIndex())
                .setTypes(elasticSearch.defaultType())
                .setQuery(termQuery("account_id", persistentAccount.getId()))
                .setSize(0)
                .setTerminateAfter(1);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Search Request {%s,%s}", elasticSearch.containerIndex(), persistentAccount.getId()));
        }


        return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultGetTimeout())
                .map(Optional::get)
                .map(searchResponse -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(format("Search Response {%s,%s} = %s", elasticSearch.containerIndex(), persistentAccount.getId(), Jsonify.toString(searchResponse)));
                    }
                    return searchResponse.getHits().getTotalHits() > 0;
                });
    }

}
