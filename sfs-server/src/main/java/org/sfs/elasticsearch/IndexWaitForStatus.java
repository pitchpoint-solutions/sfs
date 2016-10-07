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
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.rx.ToVoid;
import rx.Observable;
import rx.functions.Func1;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

public class IndexWaitForStatus implements Func1<Void, Observable<Void>> {

    private final VertxContext<Server> vertxContext;
    private final ClusterHealthStatus validStatus;
    private final String index;


    public IndexWaitForStatus(VertxContext<Server> vertxContext, String index, ClusterHealthStatus validStatus) {
        this.vertxContext = vertxContext;
        this.index = index;
        this.validStatus = validStatus;
    }

    @Override
    public Observable<Void> call(Void aVoid) {
        Elasticsearch elasticsearch = vertxContext.verticle().elasticsearch();

        ClusterHealthRequestBuilder request =
                elasticsearch.get()
                        .admin()
                        .cluster()
                        .prepareHealth(index)
                        .setWaitForStatus(validStatus)
                        .setTimeout(timeValueSeconds(2));

        return elasticsearch.execute(vertxContext, request, elasticsearch.getDefaultAdminTimeout())
                .map(Optional::get)
                .map(new ToVoid<>());
    }

}
