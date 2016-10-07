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

package org.sfs.elasticsearch;

import com.google.common.base.Optional;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.sfs.Server;
import org.sfs.VertxContext;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.collect.Iterators.toArray;
import static rx.Observable.from;

public class ListSfsIndexes implements Func1<Void, Observable<String>> {

    private final VertxContext<Server> vertxContext;

    public ListSfsIndexes(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<String> call(Void aVoid) {
        Elasticsearch elasticsearch = vertxContext.verticle().elasticsearch();

        ClusterStateRequestBuilder request =
                elasticsearch.get()
                        .admin()
                        .cluster()
                        .prepareState();
        return elasticsearch.execute(vertxContext, request, elasticsearch.getDefaultAdminTimeout())
                .map(Optional::get)
                .flatMap(clusterStateResponse -> {
                    ClusterState state = clusterStateResponse.getState();
                    MetaData metadata = state.getMetaData();
                    return from(toArray(metadata.getIndices().keysIt(), String.class));
                })
                .filter(s -> s.startsWith(elasticsearch.indexPrefix()));
    }
}
