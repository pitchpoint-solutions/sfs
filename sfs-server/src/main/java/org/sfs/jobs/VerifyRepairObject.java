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

package org.sfs.jobs;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import io.vertx.core.MultiMap;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.ScanAndScrollStreamProducer;
import org.sfs.elasticsearch.SearchHitMaintainObjectEndableWrite;
import org.sfs.io.AsyncIO;
import org.sfs.rx.Defer;
import org.sfs.vo.ObjectPath;
import rx.Observable;

import java.util.Collections;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class VerifyRepairObject extends AbstractJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(VerifyRepairObject.class);

    @Override
    public String id() {
        return Jobs.ID.VERIFY_REPAIR_OBJECT;
    }

    @Override
    public Observable<Void> executeImpl(VertxContext<Server> vertxContext, MultiMap parameters) {
        return execute0(vertxContext, parameters);
    }

    @Override
    public Observable<Void> stopImpl(VertxContext<Server> vertxContext) {
        return Defer.aVoid();
    }


    protected Observable<Void> execute0(VertxContext<Server> vertxContext, MultiMap parameters) {

        Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        String unparsedForceRemoveVolumes =
                JobParams.getFirstOptionalParam(parameters, Jobs.Parameters.FORCE_REMOVE_VOLUMES);
        Set<String> forceRemoveVolumes = Strings.isNullOrEmpty(unparsedForceRemoveVolumes)
                ? Collections.emptySet()
                : Sets.newHashSet(Splitter.on(',').omitEmptyStrings().trimResults().split(unparsedForceRemoveVolumes));

        String objectId = JobParams.getFirstRequiredParam(parameters, Jobs.Parameters.OBJECT_ID);

        ObjectPath objectPath = ObjectPath.fromPaths(objectId);
        String containerName = objectPath.containerName().get();


        String objectIndex = elasticSearch.objectIndex(containerName);

        TermQueryBuilder query = termQuery("_id", objectId);

        ScanAndScrollStreamProducer producer =
                new ScanAndScrollStreamProducer(vertxContext, query)
                        .setIndeces(objectIndex)
                        .setTypes(elasticSearch.defaultType())
                        .setReturnVersion(true);

        SearchHitMaintainObjectEndableWrite consumer = new SearchHitMaintainObjectEndableWrite(vertxContext, forceRemoveVolumes);

        LOGGER.info("Starting maintain of object " + objectId);

        return AsyncIO.pump(producer, consumer)
                .doOnNext(aVoid1 -> LOGGER.info("Finished maintain of object " + objectId));
    }
}

