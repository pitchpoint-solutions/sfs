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

package org.sfs.nodes.all.elasticsearch;

import io.vertx.core.Handler;
import org.sfs.SfsRequest;
import org.sfs.elasticsearch.IndexRefresh;
import org.sfs.rx.Terminus;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.sfs.rx.Defer.empty;

public class RefreshIndex implements Handler<SfsRequest> {

    @Override
    public void handle(SfsRequest sfsRequest) {
        empty()
                .flatMap(new IndexRefresh(sfsRequest.vertxContext()))
                .subscribe(new Terminus<Void>(sfsRequest) {
                    @Override
                    public void onNext(Void aVoid) {
                        sfsRequest.response().setStatusCode(HTTP_OK);
                    }
                });
    }
}

