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

import com.google.common.base.Optional;
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.vo.PersistentAccount;
import org.sfs.vo.PersistentContainer;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static org.sfs.vo.PersistentContainer.fromGetResponse;

public class LoadContainer implements Func1<String, Observable<Optional<PersistentContainer>>> {

    private static final Logger LOGGER = getLogger(LoadContainer.class);
    private final VertxContext<Server> vertxContext;
    private final PersistentAccount account;

    public LoadContainer(VertxContext<Server> vertxContext, PersistentAccount account) {
        this.vertxContext = vertxContext;
        this.account = account;
    }

    @Override
    public Observable<Optional<PersistentContainer>> call(String containerId) {

        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();


        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Get Request {%s,%s,%s}", elasticSearch.defaultType(), elasticSearch.containerIndex(), containerId));
        }

        GetRequestBuilder request = elasticSearch.get()
                .prepareGet(
                        elasticSearch.containerIndex(),
                        elasticSearch.defaultType(),
                        containerId);

        return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultGetTimeout())
                .map(oGetResponse -> {
                    GetResponse getResponse = oGetResponse.get();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(format("Get Response {%s,%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.containerIndex(), containerId, Jsonify.toString(getResponse)));
                    }
                    if (getResponse.isExists()) {
                        return of(fromGetResponse(account, getResponse));
                    } else {
                        return absent();
                    }
                });
    }
}