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

import com.google.common.base.Optional;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.PersistentObject;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.sfs.vo.PersistentObject.fromIndexResponse;

public class UpdateObject implements Func1<PersistentObject, Observable<Optional<PersistentObject>>> {


    private static final Logger LOGGER = getLogger(UpdateObject.class);
    private final VertxContext<Server> vertxContext;

    public UpdateObject(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<Optional<PersistentObject>> call(final PersistentObject persistentObject) {

        final JsonObject source = persistentObject.toJsonObject();

        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        PersistentContainer persistentContainer = persistentObject.getParent();

        String objectIndex = elasticSearch.objectIndex(persistentContainer.getName());

        String encoded;

        if (LOGGER.isDebugEnabled()) {
            encoded = source.encodePrettily();
            LOGGER.debug(format("Index Request {%s,%s,%s,%d} = %s", elasticSearch.defaultType(), objectIndex, persistentObject.getId(), persistentObject.getPersistentVersion(), encoded));
        } else {
            encoded = source.encode();
        }

        IndexRequestBuilder request =
                elasticSearch.get()
                        .prepareIndex(
                                objectIndex,
                                elasticSearch.defaultType(),
                                persistentObject.getId())
                        .setVersion(persistentObject.getPersistentVersion())
                        .setTimeout(timeValueMillis(elasticSearch.getDefaultIndexTimeout() - 10))
                        .setSource(encoded);

        return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultIndexTimeout())
                .map(indexResponse -> {
                    if (indexResponse.isPresent()) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("Index Response {%s,%s,%s,%d} = %s", elasticSearch.defaultType(), objectIndex, persistentObject.getId(), persistentObject.getPersistentVersion(), Jsonify.toString(indexResponse.get())));
                        }
                        return of(fromIndexResponse(persistentObject.getParent(), indexResponse.get(), source));
                    } else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("Index Response {%s,%s,%s,%d} = %s", elasticSearch.defaultType(), objectIndex, persistentObject.getId(), persistentObject.getPersistentVersion(), "null"));
                        }
                        return absent();
                    }
                });
    }
}
