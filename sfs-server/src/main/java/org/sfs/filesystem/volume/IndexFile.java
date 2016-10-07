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
import io.vertx.core.logging.Logger;
import org.sfs.SfsVertx;
import org.sfs.filesystem.BlockFile;
import org.sfs.filesystem.ChecksummedPositional;
import rx.Observable;

import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static com.google.common.collect.FluentIterable.from;
import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static org.sfs.block.RangeLock.Lock;
import static org.sfs.protobuf.XVolume.XIndexBlock;
import static org.sfs.protobuf.XVolume.XIndexBlock.parseFrom;

public class IndexFile {

    private static final Logger LOGGER = getLogger(IndexFile.class);
    private final BlockFile blockFile;

    public IndexFile(Path path, int blockSize) {
        blockFile = new BlockFile(path, blockSize);
    }

    public long getWriteQueueSize() {
        return blockFile.getWriteQueueSize();
    }

    public long getWriteQueueLowWater() {
        return blockFile.getWriteQueueLowWater();
    }

    public long getWriteQueueMaxWrites() {
        return blockFile.getWriteQueueMaxWrites();
    }

    public int getBlockSize() {
        return blockFile.getBlockSize();
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

    public int getLockCount() {
        return blockFile.getLockCount();
    }

    public Observable<Optional<ChecksummedPositional<XIndexBlock>>> getBlock(SfsVertx vertx, long position) {
        return blockFile.getBlock(vertx, position)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(this::parse)
                .doOnNext(checksummedPositionalOptional -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(format("Block @ position %d was %s", position, checksummedPositionalOptional));
                    }
                })
                .singleOrDefault(absent());
    }

    public Observable<Iterable<ChecksummedPositional<XIndexBlock>>> getBlocks(SfsVertx vertx, long position, int numberOfBlocks) {
        return blockFile.getBlocks(vertx, position, numberOfBlocks)
                .map(checksummedPositionals ->
                        from(checksummedPositionals)
                                .transform(input -> parse(input))
                                .filter(Optional::isPresent)
                                .transform(Optional::get));
    }

    public Observable<Void> open(SfsVertx vertx, StandardOpenOption openOption, StandardOpenOption... openOptions) {
        return blockFile.open(vertx, openOption, openOptions);
    }

    public Observable<Void> setBlock(SfsVertx vertx, long position, XIndexBlock data) {
        return blockFile.setBlock(vertx, position, buffer(data.toByteArray()));
    }

    public Observable<Long> size(SfsVertx vertx) {
        return blockFile.size(vertx);
    }

    public Optional<Lock> tryReadLock(long position, long length) {
        return blockFile.tryReadLock(position, length);
    }

    public Optional<Lock> tryWriteLock(long position, long length) {
        return blockFile.tryWriteLock(position, length);
    }

    public Observable<Void> force(SfsVertx vertx, boolean metaData) {
        return blockFile.force(vertx, metaData);
    }

    protected Optional<ChecksummedPositional<XIndexBlock>> parse(ChecksummedPositional<byte[]> checksummedPositional) {
        try {
            if (checksummedPositional.isChecksumValid()) {
                XIndexBlock indexBlock = parseFrom(checksummedPositional.getValue());
                return of(new ChecksummedPositional<XIndexBlock>(checksummedPositional.getPosition(), indexBlock, checksummedPositional.getActualChecksum()) {
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
