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
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.vo.PersistentContainer;
import rx.Observable;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;

public class RemoveObjectIndex implements Func1<PersistentContainer, Observable<PersistentContainer>> {

    private static final Logger LOGGER = getLogger(UpdateContainer.class);
    private final VertxContext<Server> vertxContext;

    public RemoveObjectIndex(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<PersistentContainer> call(final PersistentContainer persistentContainer) {

        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        return elasticSearch.deleteObjectIndex(vertxContext, persistentContainer.getName())
                .map(aVoid -> persistentContainer);
    }
}