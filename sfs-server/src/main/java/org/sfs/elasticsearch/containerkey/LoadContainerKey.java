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
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.rx.Holder2;
import org.sfs.rx.Holder3;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.PersistentContainerKey;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static org.sfs.vo.PersistentContainerKey.fromGetResponse;

public class LoadContainerKey implements Func1<Holder2<PersistentContainer, String>, Observable<Holder3<PersistentContainer, String, Optional<PersistentContainerKey>>>> {

    private static final Logger LOGGER = getLogger(LoadContainerKey.class);
    private final VertxContext<Server> vertxContext;

    public LoadContainerKey(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<Holder3<PersistentContainer, String, Optional<PersistentContainerKey>>> call(final Holder2<PersistentContainer, String> input) {

        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        final String id = input.value1();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Get Request {%s,%s,%s}", elasticSearch.defaultType(), elasticSearch.containerKeyIndex(), id));
        }


        GetRequestBuilder request = elasticSearch.get()
                .prepareGet(
                        elasticSearch.containerKeyIndex(),
                        elasticSearch.defaultType(),
                        id);

        return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultGetTimeout())
                .map(oGetResponse -> {
                    Holder3<PersistentContainer, String, Optional<PersistentContainerKey>> output = new Holder3<>();
                    output.value0(input.value0());
                    output.value1(input.value1());
                    GetResponse getResponse = oGetResponse.get();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(format("Get Response {%s,%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.containerKeyIndex(), id, Jsonify.toString(getResponse)));
                    }
                    if (getResponse.isExists()) {
                        output.value2 = of(fromGetResponse(input.value0(), getResponse));
                    } else {
                        output.value2 = absent();
                    }
                    return output;
                });
    }
}
