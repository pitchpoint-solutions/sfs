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

package org.sfs.filesystem;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import org.sfs.SfsVertx;
import org.sfs.rx.AsyncResultMemoizeHandler;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Boolean.TRUE;
import static org.sfs.filesystem.JournalFile.Entry;
import static org.sfs.math.Rounding.up;
import static org.sfs.rx.Defer.just;
import static rx.Observable.create;

public class JournalScanner {

    private static final Logger LOGGER = getLogger(JournalScanner.class);
    private boolean isDebugEnabled;
    private final JournalFile journalFile;
    private final int blockSize;
    private long position;

    public JournalScanner(JournalFile journalFile, long position) {
        this.journalFile = journalFile;
        this.isDebugEnabled = LOGGER.isDebugEnabled();
        this.blockSize = journalFile.getBlockSize();
        this.position = position;
    }

    public Observable<Void> scan(SfsVertx vertx, Func1<Entry, Observable<Boolean>> transformer) {
        return journalFile.size(vertx)
                .flatMap(fileSize -> {
                    AsyncResultMemoizeHandler<Void, Void> handler = new AsyncResultMemoizeHandler<>();
                    long roundedFileSize = up(fileSize, blockSize);
                    if (isDebugEnabled) {
                        long numberOfBlocks = roundedFileSize / blockSize;
                        LOGGER.debug("Max position is " + roundedFileSize + ". Expecting at most " + numberOfBlocks + " blocks");
                    }
                    scan0(vertx, transformer, handler, roundedFileSize);
                    return create(handler.subscribe);
                });
    }


    protected void scan0(SfsVertx vertx, Func1<Entry, Observable<Boolean>> transformer, Handler<AsyncResult<Void>> handler, long fileSize) {
        if (isDebugEnabled) {
            LOGGER.debug("Reading 1 blocks @ position " + position);
        }

        journalFile.getEntry(vertx, position)
                .flatMap(entryOptional -> {
                    if (entryOptional.isPresent()) {
                        Entry entry = entryOptional.get();
                        position = entry.getNextHeaderPosition();
                        return transformer.call(entry)
                                .doOnNext(_continue -> {
                                    if (isDebugEnabled) {
                                        LOGGER.debug("Scanned 1 block");
                                    }
                                })
                                .doOnNext(_continue -> position = entry.getNextHeaderPosition());
                    } else {
                        // scan forward until we find a readable block. There will
                        // eventually be a header block that can be parsed
                        // or the end of file will be reached
                        if (isDebugEnabled) {
                            LOGGER.debug("Skipped 1 block @ position " + position);
                        }
                        position += blockSize;
                        return just(true);
                    }
                })
                .subscribe(new Subscriber<Boolean>() {

                    Boolean result;

                    @Override
                    public void onCompleted() {
                        if (!TRUE.equals(result) || position >= fileSize) {
                            Future.<Void>succeededFuture().setHandler(handler);
                        } else {
                            vertx.runOnContext(event -> scan0(vertx, transformer, handler, fileSize));
                        }
                    }

                    @Override
                    public void onError(Throwable e) {
                        Future.<Void>failedFuture(e).setHandler(handler);
                    }

                    @Override
                    public void onNext(Boolean _continue) {
                        result = _continue;
                    }
                });

    }
}
