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
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.rx.ToVoid;
import org.sfs.util.StreamProducer;
import rx.Observable;
import rx.Subscriber;

import java.util.Iterator;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.search.sort.SortOrder.ASC;
import static org.elasticsearch.search.sort.SortParseElement.DOC_FIELD_NAME;
import static org.sfs.rx.Defer.empty;
import static org.sfs.rx.Defer.just;
import static rx.Observable.defer;

public class ScanAndScrollStreamProducer implements StreamProducer<SearchHit> {

    private static final Logger LOGGER = getLogger(ScanAndScrollStreamProducer.class);
    private final VertxContext<Server> vertxContext;
    private final Elasticsearch elasticsearch;
    private final QueryBuilder query;
    private Handler<SearchHit> dataHandler;
    private boolean paused;
    private Handler<Void> endHandler;
    private Handler<Throwable> exceptionHandler;
    private boolean ended = false;
    private String[] indeces = new String[0];
    private String[] types = new String[0];
    private boolean returnVersion = false;
    private String scrollId;
    private Iterator<SearchHit> searchHits;
    private boolean emitting = false;
    private boolean scrollNoHit = false;
    private boolean queried = false;
    private final Context context;
    private long count = 0;

    public ScanAndScrollStreamProducer(VertxContext<Server> vertxContext, QueryBuilder query) {
        this.vertxContext = vertxContext;
        this.elasticsearch = vertxContext.verticle().elasticsearch();
        this.query = query;
        this.context = vertxContext.vertx().getOrCreateContext();
    }

    public String[] getTypes() {
        return types;
    }

    public ScanAndScrollStreamProducer setTypes(String type, String... types) {
        this.types = new String[1 + types.length];
        this.types[0] = type;
        for (int i = 0; i < types.length; i++) {
            this.types[i + 1] = types[i];
        }
        return this;
    }

    public String[] getIndeces() {
        return indeces;
    }

    public ScanAndScrollStreamProducer setIndeces(String index, String... indeces) {
        this.indeces = new String[1 + indeces.length];
        this.indeces[0] = index;
        for (int i = 0; i < indeces.length; i++) {
            this.indeces[i + 1] = indeces[i];
        }
        return this;
    }

    public boolean isReturnVersion() {
        return returnVersion;
    }

    public ScanAndScrollStreamProducer setReturnVersion(boolean returnVersion) {
        this.returnVersion = returnVersion;
        return this;
    }

    @Override
    public ScanAndScrollStreamProducer handler(Handler<SearchHit> handler) {
        this.dataHandler = handler;
        emit();
        return this;
    }

    @Override
    public ScanAndScrollStreamProducer pause() {
        this.paused = true;
        return this;
    }

    @Override
    public ScanAndScrollStreamProducer resume() {
        this.paused = false;
        emit();
        return this;
    }

    @Override
    public ScanAndScrollStreamProducer exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        return this;
    }

    @Override
    public ScanAndScrollStreamProducer endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        handleEnd();
        return this;
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

    protected void handleError(Throwable e) {
        clearScroll().subscribe(
                new Subscriber<Void>() {
                    @Override
                    public void onCompleted() {
                        if (exceptionHandler != null) {
                            exceptionHandler.handle(e);
                        } else {
                            LOGGER.error("Unhandled Exception", e);
                        }
                    }

                    @Override
                    public void onError(Throwable e) {
                        if (exceptionHandler != null) {
                            exceptionHandler.handle(e);
                        } else {
                            LOGGER.error("Unhandled Exception", e);
                        }
                    }

                    @Override
                    public void onNext(Void aVoid) {

                    }
                });
    }

    protected void emit() {
        Handler<SearchHit> handler = dataHandler;
        if (handler == null || paused || emitting) {
            return;
        }
        if (scrollNoHit) {
            ended = true;
            handleEnd();
            return;
        }
        if (searchHits != null && searchHits.hasNext()) {
            emitting = true;
            SearchHit next = searchHits.next();
            count++;
            if (count % 1000 == 0) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Handled " + count + " records");
                }
            }
            try {
                handler.handle(next);
                context.runOnContext(event -> emit());
            } catch (Throwable e) {
                handleError(e);
            } finally {
                emitting = false;
            }
        } else

        {
            if (!queried) {
                emitting = true;
                query().subscribe(
                        new Subscriber<SearchResponse>() {

                            SearchResponse searchResponse;

                            @Override
                            public void onCompleted() {
                                queried = true;
                                scrollId = searchResponse.getScrollId();
                                searchHits = searchResponse.getHits().iterator();
                                emitting = false;
                                emit();
                            }

                            @Override
                            public void onError(Throwable e) {
                                queried = true;
                                emitting = false;
                                handleError(e);
                            }

                            @Override
                            public void onNext(SearchResponse searchResponse) {
                                this.searchResponse = searchResponse;
                            }
                        });
            } else {
                emitting = true;
                scroll().subscribe(
                        new Subscriber<SearchResponse>() {

                            SearchResponse searchResponse;

                            @Override
                            public void onCompleted() {
                                scrollId = searchResponse.getScrollId();
                                searchHits = searchResponse.getHits().iterator();
                                if (!searchHits.hasNext()) {
                                    scrollNoHit = true;
                                }
                                emitting = false;
                                emit();
                            }

                            @Override
                            public void onError(Throwable e) {
                                emitting = false;
                                handleError(e);
                            }

                            @Override
                            public void onNext(SearchResponse searchResponse) {
                                this.searchResponse = searchResponse;
                            }
                        });

            }
        }

    }

    public Observable<SearchResponse> query() {
        return defer(() -> {
            if (LOGGER.isDebugEnabled()) {
                //LOGGER.debug("Request = " + Jsonify.toString(query));
            }

            SearchRequestBuilder request =
                    elasticsearch.get()
                            .prepareSearch(indeces)
                            .setTypes(types)
                            .addSort(DOC_FIELD_NAME, ASC)
                            .setScroll(timeValueMillis(elasticsearch.getDefaultScrollTimeout()))
                            .setQuery(query)
                            .setSize(10)
                            .setTimeout(timeValueMillis(elasticsearch.getDefaultSearchTimeout() - 10))
                            .setVersion(returnVersion);

            return elasticsearch.execute(vertxContext, request, elasticsearch.getDefaultSearchTimeout())
                    .map(Optional::get)
                    .flatMap(searchResponse ->
                            just(searchResponse)
                                    .map(searchResponse1 -> {
                                        if (LOGGER.isDebugEnabled()) {
                                            LOGGER.debug("Response = " + Jsonify.toString(searchResponse1));
                                        }
                                        return searchResponse1;
                                    }));
        });

    }

    protected Observable<SearchResponse> scroll() {
        return defer(() -> {
            if (scrollId == null) {
                return just(null);
            }

            SearchScrollRequestBuilder request = elasticsearch.get()
                    .prepareSearchScroll(scrollId)
                    .setScroll(timeValueMillis(elasticsearch.getDefaultScrollTimeout()));

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Request = " + Jsonify.toString(request));
            }

            return elasticsearch.execute(vertxContext, request, elasticsearch.getDefaultSearchTimeout())
                    .map(Optional::get)
                    .flatMap(searchResponse -> just(searchResponse)
                            .map(searchResponse1 -> {
                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug("Response = " + Jsonify.toString(searchResponse1));
                                }
                                return searchResponse1;
                            }));
        });
    }

    protected Observable<Void> clearScroll() {
        return defer(() -> {
            if (scrollId == null) {
                return empty();
            }
            ClearScrollRequestBuilder request = elasticsearch.get().prepareClearScroll()
                    .addScrollId(scrollId);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Request = " + Jsonify.toString(request));
            }

            return elasticsearch.execute(vertxContext, request, elasticsearch.getDefaultGetTimeout())
                    .onErrorResumeNext(throwable -> {
                        LOGGER.warn("Handling Clear Scroll Error", throwable);
                        return just(null);
                    })
                    .map(new ToVoid<>());
        });
    }
}
