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

package org.sfs.elasticsearch.account;

import com.google.common.base.Optional;
import io.vertx.core.MultiMap;
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.search.SearchHits;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.AuthProviderService;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.elasticsearch.container.LoadContainerStats;
import org.sfs.rx.ToVoid;
import org.sfs.vo.ContainerList;
import org.sfs.vo.ObjectPath;
import org.sfs.vo.PersistentAccount;
import org.sfs.vo.PersistentContainer;
import rx.Observable;
import rx.functions.Func1;

import java.util.NavigableMap;
import java.util.TreeMap;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.primitives.Ints.tryParse;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Integer.valueOf;
import static java.lang.String.format;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.search.sort.SortOrder.ASC;
import static org.elasticsearch.search.sort.SortParseElement.DOC_FIELD_NAME;
import static org.sfs.rx.Defer.just;
import static org.sfs.util.SfsHttpQueryParams.DELIMITER;
import static org.sfs.util.SfsHttpQueryParams.END_MARKER;
import static org.sfs.util.SfsHttpQueryParams.LIMIT;
import static org.sfs.util.SfsHttpQueryParams.MARKER;
import static org.sfs.util.SfsHttpQueryParams.PREFIX;
import static org.sfs.util.UrlScaper.unescape;
import static org.sfs.vo.ContainerList.SparseContainer;
import static org.sfs.vo.ObjectPath.DELIMITER_LENGTH;
import static org.sfs.vo.PersistentContainer.fromSearchHit;
import static rx.Observable.empty;
import static rx.Observable.from;

public class ListContainers implements Func1<PersistentAccount, Observable<ContainerList>> {

    private static final Logger LOGGER = getLogger(ListContainers.class);
    private final SfsRequest sfsRequest;
    private final VertxContext<Server> vertxContext;
    private AuthProviderService authProviderService;

    public ListContainers(SfsRequest sfsRequest) {
        this.sfsRequest = sfsRequest;
        this.vertxContext = sfsRequest.vertxContext();
        this.authProviderService = vertxContext.verticle().authProviderService();
    }

    @Override
    public Observable<ContainerList> call(PersistentAccount account) {
        MultiMap queryParams = sfsRequest.params();
        Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();


        final String limit = queryParams.get(LIMIT);
        String marker = unescape(queryParams.get(MARKER));
        String endMarker = unescape(queryParams.get(END_MARKER));
        final String prefix = unescape(queryParams.get(PREFIX));
        final String delimiter = unescape(queryParams.get(DELIMITER));

        Integer parsedLimit = !isNullOrEmpty(limit) ? tryParse(limit) : valueOf(10000);
        parsedLimit = parsedLimit == null || parsedLimit < 0 || parsedLimit > 10000 ? 10000 : parsedLimit;

        String accountId = account.getId();

        String accountPrefix = accountId + ObjectPath.DELIMITER;
        if (!isNullOrEmpty(prefix)) {
            accountPrefix += prefix;
        }

        final SearchRequestBuilder scrollRequest = elasticSearch.get()
                .prepareSearch(elasticSearch.containerIndex())
                .setTypes(elasticSearch.defaultType())
                .addSort(DOC_FIELD_NAME, ASC)
                .setScroll(timeValueMillis(elasticSearch.getDefaultScrollTimeout()))
                .setTimeout(timeValueMillis(elasticSearch.getDefaultSearchTimeout() - 10))
                .setQuery(prefixQuery("_id", accountPrefix))
                .setSize(100);

        final Integer finalParsedLimit = parsedLimit;

        final NavigableMap<String, SparseContainer> listedObjects = new TreeMap<>();
        return scan(account, prefix, delimiter, marker, endMarker, finalParsedLimit, elasticSearch, scrollRequest, listedObjects)
                .map(aVoid -> new ContainerList(account, listedObjects.values()));
    }

    protected Observable<Void> scan(
            final PersistentAccount account,
            final String prefix,
            final String delimiter,
            final String marker,
            final String endMarker,
            final int limit,
            final Elasticsearch elasticsearch,
            final SearchRequestBuilder scrollRequest,
            final NavigableMap<String, SparseContainer> listedContainers) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Search Request = %s", Jsonify.toString(scrollRequest)));
        }

        return elasticsearch.execute(vertxContext, scrollRequest, elasticsearch.getDefaultSearchTimeout())
                .map(Optional::get)
                .flatMap(searchResponse -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(format("Search Response = %s", Jsonify.toString(searchResponse)));
                    }
                    SearchHits hits = searchResponse.getHits();
                    return toIterable(account, prefix, delimiter, marker, endMarker, hits)
                            .doOnNext(sparseContainer -> {
                                String id = sparseContainer.getContainerName();
                                SparseContainer existing = listedContainers.get(id);
                                if (existing == null) {
                                    listedContainers.put(id, sparseContainer);
                                } else {
                                    existing.setByteCount(existing.getByteCount() + sparseContainer.getByteCount());
                                    existing.setObjectCount(existing.getObjectCount() + sparseContainer.getObjectCount());
                                }
                                if (listedContainers.size() > limit) {
                                    listedContainers.pollLastEntry();
                                }
                            })
                            .count()
                            .map(new ToVoid<>())
                            .flatMap(aVoid -> scroll(account, prefix, delimiter, marker, endMarker, limit, elasticsearch, searchResponse.getScrollId(), listedContainers));
                });
    }

    protected Observable<Void> scroll(
            final PersistentAccount account,
            final String prefix,
            final String delimiter,
            final String marker,
            final String endMarker,
            final int limit,
            final Elasticsearch elasticsearch,
            final String scrollId,
            final NavigableMap<String, SparseContainer> listedContainers) {
        SearchScrollRequestBuilder scrollRequest =
                elasticsearch.get().prepareSearchScroll(scrollId)
                        .setScroll(timeValueMillis(elasticsearch.getDefaultScrollTimeout()));

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Search Request = %s", Jsonify.toString(scrollRequest)));
        }

        return elasticsearch.execute(vertxContext, scrollRequest, elasticsearch.getDefaultSearchTimeout())
                .map(Optional::get)
                .flatMap(searchResponse -> {

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(format("Search Response = %s", Jsonify.toString(searchResponse)));
                    }
                    SearchHits hits = searchResponse.getHits();
                    int numberOfHits = hits.getHits().length;
                    if (numberOfHits > 0) {
                        return toIterable(account, prefix, delimiter, marker, endMarker, hits)
                                .doOnNext(sparseContainer -> {
                                    String id = sparseContainer.getContainerName();
                                    SparseContainer existing = listedContainers.get(id);
                                    if (existing == null) {
                                        listedContainers.put(id, sparseContainer);
                                    } else {
                                        existing.setByteCount(existing.getByteCount() + sparseContainer.getByteCount());
                                        existing.setObjectCount(existing.getObjectCount() + sparseContainer.getObjectCount());
                                    }
                                    if (listedContainers.size() > limit) {
                                        listedContainers.pollLastEntry();
                                    }
                                })
                                .count()
                                .map(new ToVoid<>())
                                .flatMap(aVoid -> scroll(account, prefix, delimiter, marker, endMarker, limit, elasticsearch, searchResponse.getScrollId(), listedContainers));
                    } else {
                        return clearScroll(elasticsearch, searchResponse.getScrollId());
                    }
                });
    }

    protected Observable<Void> clearScroll(Elasticsearch elasticSearch, String scrollId) {
        ClearScrollRequestBuilder request =
                elasticSearch.get()
                        .prepareClearScroll()
                        .addScrollId(scrollId);
        return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultSearchTimeout())
                .onErrorResumeNext(throwable -> {
                    LOGGER.warn("Handling Clear Scroll Error", throwable);
                    return just(null);
                })
                .map(clearScrollResponseOptional -> null);
    }

    protected Observable<SparseContainer> toIterable(final PersistentAccount account, final String prefix, final String delimiter, final String marker, final String endMarker, SearchHits searchHits) {
        // container id looks like /account/container
        // object id looks like /account/container/a/b/c/1/2/3
        // which makes the start index of the object name is the length of the
        // container id + 1
        int containerNameStartIndex = account.getId().length() + DELIMITER_LENGTH;
        return from(searchHits)
                .flatMap(searchHit -> {

                    String containerId = searchHit.getId();
                    containerId = containerId.substring(containerNameStartIndex, containerId.length());
                    if (delimiter != null) {
                        int prefixLength = prefix != null ? prefix.length() : 0;
                        int containerIdLength = containerId.length();
                        if (containerIdLength > prefixLength) {
                            int indexOfDelimiter = containerId.indexOf(delimiter, prefixLength);
                            if (indexOfDelimiter <= containerIdLength && indexOfDelimiter >= 0) {
                                containerId = containerId.substring(0, indexOfDelimiter);
                            }
                        }
                    }

                    boolean isInRange = (marker == null || containerId.compareTo(marker) > 0)
                            && (endMarker == null || containerId.compareTo(endMarker) < 0);

                    if (isInRange) {
                        PersistentContainer persistentContainer = fromSearchHit(account, searchHit);
                        String finalContainerId = containerId;
                        return authProviderService.canContainerRead(sfsRequest, persistentContainer)
                                .filter(canDo -> canDo)
                                .map(canDo -> persistentContainer)
                                .flatMap(new LoadContainerStats(vertxContext))
                                .map(containerStats -> {
                                    long objectCount = containerStats.getObjectCount();
                                    double bytesUsed = containerStats.getBytesUsed();
                                    SparseContainer sparseContainer = new SparseContainer(finalContainerId, objectCount, bytesUsed);
                                    return sparseContainer;
                                });
                    }
                    return empty();
                });
    }
}