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

package org.sfs.filesystem.volume;

import com.google.common.base.Optional;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import org.sfs.SfsVertx;
import org.sfs.filesystem.BlockFile;
import org.sfs.filesystem.ChecksummedPositional;
import rx.Observable;

import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Long.MAX_VALUE;
import static org.sfs.block.RangeLock.Lock;
import static org.sfs.protobuf.XVolume.XSuperBlock;
import static org.sfs.protobuf.XVolume.XSuperBlock.parseFrom;
import static org.sfs.rx.RxHelper.combineSinglesDelayError;
import static rx.Observable.just;

public class MetaFile {

    private static final Logger LOGGER = getLogger(MetaFile.class);
    private static final int BLOCK_SIZE = 1048576;
    private final BlockFile blockFile;
    private final long position0;
    private final long position1;

    public MetaFile(Path path) {
        blockFile = new BlockFile(path, BLOCK_SIZE);
        position0 = BLOCK_SIZE;
        position1 = 0 + BLOCK_SIZE;
    }

    public int getLockCount() {
        return blockFile.getLockCount();
    }

    public Observable<Void> close(SfsVertx vertx) {
        return blockFile.close(vertx);
    }

    public Observable<Void> disableWrites(SfsVertx vertx) {
        return blockFile.disableWrites(vertx);
    }

    public Observable<Void> enableWrites(SfsVertx vertx) {
        return blockFile.enableWrites(vertx);
    }

    public Observable<Void> open(SfsVertx vertx, StandardOpenOption openOption, StandardOpenOption... openOptions) {
        return blockFile.open(vertx, openOption, openOptions);
    }

    public Observable<Void> set(SfsVertx vertx, XSuperBlock data) {
        Buffer buffer = buffer(data.toByteArray());
        // write two copies so that we can read from position0 or position1 in
        // cases where one is corrupt
        return combineSinglesDelayError(
                blockFile.setBlock(vertx, position0, buffer),
                blockFile.setBlock(vertx, position1, buffer),
                (aVoid, aVoid2) -> (Void) null)
                .flatMap(aVoid -> blockFile.force(vertx, true));
    }

    public Observable<Long> size(SfsVertx vertx) {
        return blockFile.size(vertx);
    }

    public Optional<Lock> tryReadLock() {
        return blockFile.tryReadLock(0, MAX_VALUE);
    }

    public Optional<Lock> tryWriteLock() {
        return blockFile.tryWriteLock(0, MAX_VALUE);
    }

    public Observable<Optional<XSuperBlock>> getBlock(SfsVertx vertx) {
        // attempt to read position0, if position1 fails then attempt to read position1
        return getBlock0(vertx, position0)
                .flatMap(xSuperBlockOptional -> {
                    if (xSuperBlockOptional.isPresent()) {
                        return just(xSuperBlockOptional);
                    } else {
                        return getBlock0(vertx, position1);
                    }
                });
    }

    private Observable<Optional<XSuperBlock>> getBlock0(SfsVertx vertx, long position) {
        return blockFile.getBlock(vertx, position)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(this::parse)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(xSuperBlockChecksummedPositional -> of(xSuperBlockChecksummedPositional.getValue()))
                .singleOrDefault(absent());
    }

    public Observable<Void> force(SfsVertx vertx, boolean metaData) {
        return blockFile.force(vertx, metaData);
    }

    protected Optional<ChecksummedPositional<XSuperBlock>> parse(ChecksummedPositional<byte[]> checksummedPositional) {
        try {
            if (checksummedPositional.isChecksumValid()) {
                XSuperBlock indexBlock = parseFrom(checksummedPositional.getValue());
                return of(new ChecksummedPositional<XSuperBlock>(checksummedPositional.getPosition(), indexBlock, checksummedPositional.getActualChecksum()) {
                    @Override
                    public boolean isChecksumValid() {
                        return true;
                    }
                });
            } else {
                LOGGER.warn("Invalid checksum for index block @ position " + checksummedPositional.getPosition());
                return absent();
            }
        } catch (Throwable e) {
            LOGGER.warn("Error parsing index block @ position " + checksummedPositional.getPosition(), e);
            return absent();
        }

    }
}
