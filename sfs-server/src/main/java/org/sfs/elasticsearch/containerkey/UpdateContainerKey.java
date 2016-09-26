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
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.elasticsearch.container.UpdateContainer;
import org.sfs.rx.Holder2;
import org.sfs.vo.PersistentContainerKey;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.sfs.vo.PersistentContainerKey.fromIndexResponse;

public class UpdateContainerKey implements Func1<PersistentContainerKey, Observable<Holder2<PersistentContainerKey, Optional<PersistentContainerKey>>>> {

    private static final Logger LOGGER = getLogger(UpdateContainer.class);
    private final VertxContext<Server> vertxContext;

    public UpdateContainerKey(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<Holder2<PersistentContainerKey, Optional<PersistentContainerKey>>> call(final PersistentContainerKey persistentContainerKey) {
        final JsonObject source = persistentContainerKey.toJsonObject();

        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        String encoded;

        if (LOGGER.isDebugEnabled()) {
            encoded = source.encodePrettily();
            LOGGER.debug(format("Index Request {%s,%s,%s,%d} = %s", elasticSearch.defaultType(), elasticSearch.containerKeyIndex(), persistentContainerKey.getId(), persistentContainerKey.getPersistentVersion(), encoded));
        } else {
            encoded = source.encode();
        }

        IndexRequestBuilder request =
                elasticSearch.get()
                        .prepareIndex(
                                elasticSearch.containerKeyIndex(),
                                elasticSearch.defaultType(),
                                persistentContainerKey.getId())
                        .setVersion(persistentContainerKey.getPersistentVersion())
                        .setTimeout(timeValueMillis(elasticSearch.getDefaultIndexTimeout() - 10))
                        .setSource(encoded);

        return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultIndexTimeout())
                .map(indexResponse -> {
                    Holder2<PersistentContainerKey, Optional<PersistentContainerKey>> output = new Holder2<>();
                    output.value0 = persistentContainerKey;
                    if (indexResponse.isPresent()) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("Index Response {%s,%s,%s,%d} = %s", elasticSearch.defaultType(), elasticSearch.containerKeyIndex(), persistentContainerKey.getId(), persistentContainerKey.getPersistentVersion(), Jsonify.toString(indexResponse.get())));
                        }
                        output.value1 = of(fromIndexResponse(persistentContainerKey.getPersistentContainer(), indexResponse.get(), source));
                    } else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("Index Response {%s,%s,%s,%d} = %s", elasticSearch.defaultType(), elasticSearch.containerKeyIndex(), persistentContainerKey.getId(), persistentContainerKey.getPersistentVersion(), "null"));
                        }
                        output.value1 = absent();
                    }
                    return output;
                });
    }
}
