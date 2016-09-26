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
import org.sfs.vo.ObjectPath;
import org.sfs.vo.PersistentContainer;
import rx.Observable;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static org.sfs.util.Limits.NOT_SET;
import static org.sfs.vo.ObjectPath.fromPaths;

public class CreateObjectIndex implements Func1<PersistentContainer, Observable<PersistentContainer>> {

    private static final Logger LOGGER = getLogger(UpdateContainer.class);
    private final VertxContext<Server> vertxContext;
    private int numberOfShards = NOT_SET;
    private int numberOfReplicas = NOT_SET;

    public CreateObjectIndex(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    public CreateObjectIndex setNumberOfShards(int numberOfShards) {
        this.numberOfShards = numberOfShards;
        return this;
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public CreateObjectIndex setNumberOfReplicas(int numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
        return this;
    }

    @Override
    public Observable<PersistentContainer> call(final PersistentContainer persistentContainer) {

        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        ObjectPath objectPath = fromPaths(persistentContainer.getId());
        String containerName = objectPath.containerName().get();
        return elasticSearch.prepareObjectIndex(vertxContext, containerName, numberOfShards, numberOfReplicas)
                .map(aVoid -> persistentContainer);
    }
}