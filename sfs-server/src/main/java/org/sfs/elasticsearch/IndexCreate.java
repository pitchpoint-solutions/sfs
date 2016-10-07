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
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.sfs.Server;
import org.sfs.VertxContext;
import rx.Observable;
import rx.functions.Func1;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;

public class IndexCreate implements Func1<String, Observable<Boolean>> {

    private static final Logger LOGGER = getLogger(IndexCreate.class);
    private final VertxContext<Server> vertxContext;
    private Settings settings;
    private Map<String, String> mappings = new HashMap<>();

    public IndexCreate(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    public IndexCreate withMapping(String type, String mapping) {
        checkState(mappings.putIfAbsent(type, mapping) == null, "Mapping for type %s already exists", type);
        return this;
    }

    public Settings getSettings() {
        return settings;
    }

    public IndexCreate setSettings(Settings settings) {
        this.settings = settings;
        return this;
    }

    @Override
    public Observable<Boolean> call(String index) {
        Elasticsearch elasticsearch = vertxContext.verticle().elasticsearch();
        CreateIndexRequestBuilder request = elasticsearch.get().admin().indices().prepareCreate(index);
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            String type = entry.getKey();
            String mapping = entry.getValue();
            request = request.addMapping(type, mapping);
        }
        if (settings != null) {
            request.setSettings(settings);
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Request %s", Jsonify.toString(request)));
        }

        return elasticsearch.execute(vertxContext, request, elasticsearch.getDefaultAdminTimeout())
                .map(Optional::get)
                .doOnNext(createIndexResponse -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(format("Response %s", Jsonify.toString(createIndexResponse)));
                    }
                })
                .map(AcknowledgedResponse::isAcknowledged);
    }
}
