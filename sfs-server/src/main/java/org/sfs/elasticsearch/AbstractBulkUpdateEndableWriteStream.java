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
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.io.EndableWriteStream;
import org.sfs.rx.Sleep;
import rx.Observable;
import rx.Subscriber;

import static com.google.common.base.Objects.equal;
import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static rx.Observable.just;

public abstract class AbstractBulkUpdateEndableWriteStream implements EndableWriteStream<SearchHit> {

    private static final Logger LOGGER = getLogger(AbstractBulkUpdateEndableWriteStream.class);
    private final Elasticsearch elasticsearch;
    protected final VertxContext<Server> vertxContext;
    private Handler<Throwable> exceptionHandler;
    private Handler<Void> drainHandler;
    private Handler<Void> endHandler;
    private int writeQueueMaxSize = 100;
    private boolean writeQueueFull = false;
    private boolean ended = false;
    private BulkRequestBuilder bulkRequest;
    private long count = 0;

    public AbstractBulkUpdateEndableWriteStream(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
        this.elasticsearch = vertxContext.verticle().elasticsearch();
    }

    @Override
    public AbstractBulkUpdateEndableWriteStream drainHandler(Handler<Void> handler) {
        checkNotEnded();
        drainHandler = handler;
        handleDrain();
        return this;
    }

    @Override
    public AbstractBulkUpdateEndableWriteStream write(SearchHit data) {
        checkWriteQueueNotFull();
        checkNotEnded();
        writeQueueFull = true;
        write0(data)
                .subscribe(new Subscriber<Void>() {
                    @Override
                    public void onCompleted() {
                        flush();
                    }

                    @Override
                    public void onError(Throwable e) {
                        writeQueueFull = false;
                        handleError(e);
                    }

                    @Override
                    public void onNext(Void aVoid) {

                    }
                });

        return this;
    }

    @Override
    public AbstractBulkUpdateEndableWriteStream exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        return this;
    }

    @Override
    public AbstractBulkUpdateEndableWriteStream setWriteQueueMaxSize(int maxSize) {
        writeQueueMaxSize = maxSize;
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return writeQueueFull;
    }

    @Override
    public AbstractBulkUpdateEndableWriteStream endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        handleEnd();
        return this;
    }

    @Override
    public void end(SearchHit data) {
        checkWriteQueueNotFull();
        checkNotEnded();
        ended = true;
        write0(data)
                .subscribe(new Subscriber<Void>() {
                    @Override
                    public void onCompleted() {
                        flush();
                    }

                    @Override
                    public void onError(Throwable e) {
                        writeQueueFull = false;
                        handleError(e);
                    }

                    @Override
                    public void onNext(Void aVoid) {

                    }
                });
    }

    @Override
    public void end() {
        checkWriteQueueNotFull();
        checkNotEnded();
        ended = true;
        flush();
    }

    private void checkNotEnded() {
        checkState(!ended, "Already ended");
    }

    private void checkWriteQueueNotFull() {
        checkState(!writeQueueFull, "Write Queue Full");
    }

    protected void handleError(Throwable e) {
        if (exceptionHandler != null) {
            exceptionHandler.handle(e);
        } else {
            LOGGER.error("Unhandled Exception", e);
        }

    }

    protected void handleEnd() {
        if (ended) {
            if (endHandler != null) {
                Handler<Void> handler = endHandler;
                endHandler = null;
                vertxContext.vertx().runOnContext(event -> handler.handle(null));
            }
        }
    }

    protected void handleDrain() {
        if (drainHandler != null && !writeQueueFull()) {
            Handler<Void> handler = drainHandler;
            drainHandler = null;
            vertxContext.vertx().runOnContext(event -> handler.handle(null));
        }
    }

    protected abstract Observable<Optional<JsonObject>> transform(JsonObject data, String id, long version);

    protected Observable<Void> write0(SearchHit data) {
        count++;
        Observable<Void> o;
        if (count % 1000 == 0) {
            o = just((Void) null)
                    .flatMap(new Sleep(vertxContext, 1000));
        } else {
            o = just(null);
        }
        return o.flatMap(aVoid -> {
            String index = data.getIndex();
            String type = data.getType();
            String id = data.getId();
            long version = data.getVersion();
            JsonObject jsonObject = new JsonObject(data.getSourceAsString());
            return transform(jsonObject, id, version)
                    .map(oUpdatedJsonObject -> {
                        if (oUpdatedJsonObject.isPresent()) {
                            JsonObject updatedJsonObject = oUpdatedJsonObject.get();

                            if (!equal(updatedJsonObject, jsonObject)) {

                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug(format("Index Request {%s,%s,%s,%d} = %s, original was %s", index, type, id, data.getVersion(), updatedJsonObject.encodePrettily(), jsonObject.encodePrettily()));
                                }

                                IndexRequestBuilder request = elasticsearch.get()
                                        .prepareIndex(index, type, id)
                                        .setSource(updatedJsonObject.encode())
                                        .setTimeout(timeValueMillis(elasticsearch.getDefaultIndexTimeout() - 10));
                                if (version >= 0) {
                                    request = request.setVersion(version);
                                }

                                if (bulkRequest == null) {
                                    bulkRequest = elasticsearch.get().prepareBulk()
                                            .setTimeout(timeValueMillis((elasticsearch.getDefaultIndexTimeout() * writeQueueMaxSize) - 10));
                                }
                                bulkRequest.add(request);

                            }
                        } else {
                            DeleteRequestBuilder request = elasticsearch.get()
                                    .prepareDelete(index, type, id)
                                    .setTimeout(timeValueMillis(elasticsearch.getDefaultDeleteTimeout() - 10));
                            if (version >= 0) {
                                request = request.setVersion(version);
                            }

                            if (bulkRequest == null) {
                                bulkRequest = elasticsearch.get().prepareBulk()
                                        .setTimeout(timeValueMillis((elasticsearch.getDefaultIndexTimeout() * writeQueueMaxSize) - 10));
                            }
                            bulkRequest.add(request);
                        }
                        return (Void) null;
                    })
                    .timeout(120, SECONDS, vertxContext.rxVertx().contextScheduler())
                    .onErrorResumeNext(throwable -> {
                        LOGGER.warn("Handling Error", throwable);
                        return just(null);
                    })
                    .singleOrDefault(null);
        });
    }

    protected void flush() {
        if (bulkRequest != null && (ended || bulkRequest.numberOfActions() > writeQueueMaxSize)) {
            elasticsearch.execute(vertxContext, bulkRequest, (elasticsearch.getDefaultIndexTimeout() * writeQueueMaxSize))
                    .map(Optional::get)
                    .subscribe(new Subscriber<BulkResponse>() {

                        @Override
                        public void onCompleted() {
                            writeQueueFull = false;
                            bulkRequest = null;
                            if (ended) {
                                handleEnd();
                            } else {
                                handleDrain();
                            }
                        }

                        @Override
                        public void onError(Throwable e) {
                            writeQueueFull = false;
                            bulkRequest = null;
                            handleError(e);
                        }

                        @Override
                        public void onNext(BulkResponse bulkItemResponses) {
                            boolean hasFailed = false;
                            for (BulkItemResponse response : bulkItemResponses.getItems()) {
                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug(format("Index Response {%s,%s,%s,%d}", response.getIndex(), response.getType(), response.getId(), response.getVersion()));
                                }
                                if (response.isFailed()) {
                                    hasFailed |= true;
                                    LOGGER.error(format("Update of %s with id %s failed. Reason was %s", response.getType(), response.getId(), response.getFailureMessage()));
                                }
                            }
                            if (hasFailed) {
                                throw new BulkUpdateFailedException();
                            }
                        }
                    });
        } else {
            writeQueueFull = false;
            if (ended) {
                handleEnd();
            } else {
                handleDrain();
            }
        }
    }

    public static class BulkUpdateFailedException extends RuntimeException {

    }
}
