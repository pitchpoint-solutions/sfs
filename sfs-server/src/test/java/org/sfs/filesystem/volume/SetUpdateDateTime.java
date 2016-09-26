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

package org.sfs.filesystem.volume;

import com.google.common.base.Optional;
import io.vertx.core.logging.Logger;
import org.sfs.SfsVertx;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.filesystem.ChecksummedPositional;
import rx.Observable;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static org.sfs.protobuf.XVolume.XIndexBlock;

public class SetUpdateDateTime implements Func1<Long, Observable<Long>> {

    private static final Logger LOGGER = getLogger(SetUpdateDateTime.class);
    private final SfsVertx vertx;
    private final VolumeV1 sfsDataV1;
    private final long updateTs;

    public SetUpdateDateTime(SfsVertx vertx, VolumeV1 sfsDataV1, long updateTs) {
        this.vertx = vertx;
        this.sfsDataV1 = sfsDataV1;
        this.updateTs = updateTs;
    }

    @Override
    public Observable<Long> call(final Long position) {
        return sfsDataV1.getIndexBlock0(vertx, position)
                .flatMap(new Func1<Optional<ChecksummedPositional<XIndexBlock>>, Observable<Long>>() {
                    @Override
                    public Observable<Long> call(Optional<ChecksummedPositional<XIndexBlock>> versionedPositionalOptional) {
                        XIndexBlock xHeader = versionedPositionalOptional.get().getValue();
                        LOGGER.debug("Header=" + Jsonify.toString(xHeader));
                        xHeader = xHeader.toBuilder().setUpdatedTs(updateTs).build();
                        final XIndexBlock finalXHeader = xHeader;
                        return sfsDataV1.setIndexBlock0(vertx, versionedPositionalOptional.get().getPosition(), xHeader)
                                .map(new Func1<Void, Long>() {
                                    @Override
                                    public Long call(Void aVoid) {
                                        LOGGER.debug("Header=" + Jsonify.toString(finalXHeader));
                                        return position;
                                    }
                                });
                    }
                });
    }

}