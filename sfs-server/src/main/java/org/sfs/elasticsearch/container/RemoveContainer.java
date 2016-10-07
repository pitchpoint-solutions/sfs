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

package org.sfs.elasticsearch.container;

import com.google.common.base.Optional;
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.vo.PersistentContainer;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class RemoveContainer implements Func1<PersistentContainer, Observable<Optional<PersistentContainer>>> {

    private static final Logger LOGGER = getLogger(RemoveContainer.class);
    private final VertxContext<Server> vertxContext;

    public RemoveContainer(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<Optional<PersistentContainer>> call(PersistentContainer persistentContainer) {
        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Remove Request {%s,%s,%s,%d}", elasticSearch.defaultType(), elasticSearch.containerIndex(), persistentContainer.getId(), persistentContainer.getPersistentVersion()));
        }


        DeleteRequestBuilder request =
                elasticSearch.get()
                        .prepareDelete(
                                elasticSearch.containerIndex(),
                                elasticSearch.defaultType(),
                                persistentContainer.getId())
                        .setTimeout(timeValueMillis(elasticSearch.getDefaultDeleteTimeout() - 10))
                        .setVersion(persistentContainer.getPersistentVersion());

        return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultDeleteTimeout())
                .map(oDeleteResponse -> {
                    if (oDeleteResponse.isPresent()) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("Remove Response {%s,%s,%s,%d} = %s", elasticSearch.defaultType(), elasticSearch.containerIndex(), persistentContainer.getId(), persistentContainer.getPersistentVersion(), Jsonify.toString(oDeleteResponse.get())));
                        }
                        return of(persistentContainer);
                    } else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("Remove Response {%s,%s,%s,%d} = %s", elasticSearch.defaultType(), elasticSearch.containerIndex(), persistentContainer.getId(), persistentContainer.getPersistentVersion(), "null"));
                        }
                        return absent();
                    }
                });
    }
}
