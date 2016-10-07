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

package org.sfs.encryption;

import com.google.common.base.Optional;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.AbstractBulkUpdateEndableWriteStream;
import org.sfs.rx.Holder2;
import org.sfs.vo.PersistentMasterKey;
import rx.Observable;

import static com.google.common.base.Optional.of;
import static io.vertx.core.logging.LoggerFactory.getLogger;

public class MasterKeyCheckEndableWriteStream extends AbstractBulkUpdateEndableWriteStream {

    private static final Logger LOGGER = getLogger(MasterKeyCheckEndableWriteStream.class);
    private static final byte[] EMPTY_ARRAY = new byte[]{};
    private MasterKeys masterKeys;

    public MasterKeyCheckEndableWriteStream(VertxContext<Server> vertxContext) {
        super(vertxContext);
        masterKeys = vertxContext.verticle().masterKeys();
    }

    @Override
    protected Observable<Optional<JsonObject>> transform(JsonObject data, String id, long version) {

        PersistentMasterKey persistentMasterKey = new PersistentMasterKey(id, version).merge(data);

        return masterKeys.tryRepair(vertxContext, persistentMasterKey)
                .map(Holder2::value0)
                .map(persistentMasterKey1 -> of(persistentMasterKey1.toJsonObject()));
    }
}