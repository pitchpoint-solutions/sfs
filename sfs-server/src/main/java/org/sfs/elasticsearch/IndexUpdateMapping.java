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

package org.sfs.elasticsearch;

import com.google.common.base.Optional;
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.sfs.Server;
import org.sfs.VertxContext;
import rx.Observable;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;

public class IndexUpdateMapping implements Func1<String, Observable<Boolean>> {

    private static final Logger LOGGER = getLogger(IndexUpdateMapping.class);
    private final VertxContext<Server> vertxContext;
    private final String type;
    private final String mapping;

    public IndexUpdateMapping(VertxContext<Server> vertxContext, String type, String mapping) {
        this.vertxContext = vertxContext;
        this.type = type;
        this.mapping = mapping;
    }


    @Override
    public Observable<Boolean> call(String index) {
        Elasticsearch elasticsearch = vertxContext.verticle().elasticsearch();
        PutMappingRequestBuilder request = elasticsearch.get().admin().indices()
                .preparePutMapping(index)
                .setSource(mapping)
                .setType(type);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Request %s", Jsonify.toString(request)));
        }
        return elasticsearch.execute(vertxContext, request, elasticsearch.getDefaultAdminTimeout())
                .map(Optional::get)
                .doOnNext(putMappingResponse -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(format("Response %s", Jsonify.toString(putMappingResponse)));
                    }
                })
                .map(AcknowledgedResponse::isAcknowledged);
    }
}
