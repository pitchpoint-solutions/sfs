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

package org.sfs.elasticsearch.container;

import com.google.common.base.Optional;
import io.vertx.core.MultiMap;
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchHits;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.AuthProviderService;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.rx.Defer;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpRequestValidationException;
import org.sfs.validate.ValidateVersionHasSegments;
import org.sfs.validate.ValidateVersionIsReadable;
import org.sfs.validate.ValidateVersionNotDeleteMarker;
import org.sfs.validate.ValidateVersionNotDeleted;
import org.sfs.validate.ValidateVersionNotExpired;
import org.sfs.validate.ValidateVersionSegmentsHasData;
import org.sfs.vo.ObjectList;
import org.sfs.vo.ObjectPath;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.PersistentObject;
import org.sfs.vo.TransientVersion;
import rx.Observable;
import rx.functions.Func1;

import java.util.Calendar;
import java.util.NavigableMap;
import java.util.TreeMap;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.MediaType.OCTET_STREAM;
import static com.google.common.primitives.Ints.tryParse;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Integer.valueOf;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.search.sort.SortOrder.ASC;
import static org.elasticsearch.search.sort.SortParseElement.DOC_FIELD_NAME;
import static org.sfs.util.ExceptionHelper.containsException;
import static org.sfs.util.SfsHttpQueryParams.DELIMITER;
import static org.sfs.util.SfsHttpQueryParams.END_MARKER;
import static org.sfs.util.SfsHttpQueryParams.LIMIT;
import static org.sfs.util.SfsHttpQueryParams.MARKER;
import static org.sfs.util.SfsHttpQueryParams.PREFIX;
import static org.sfs.util.UrlScaper.unescape;
import static org.sfs.vo.ObjectPath.DELIMITER_LENGTH;
import static org.sfs.vo.PersistentObject.fromSearchHit;
import static org.sfs.vo.Segment.EMPTY_MD5;
import static rx.Observable.empty;
import static rx.Observable.error;
import static rx.Observable.from;
import static rx.Observable.just;

public class ListObjects implements Func1<PersistentContainer, Observable<ObjectList>> {

    private static final Logger LOGGER = getLogger(ListObjects.class);
    private final SfsRequest sfsRequest;
    private final VertxContext<Server> vertxContext;
    private final AuthProviderService authProviderService;

    public ListObjects(SfsRequest sfsRequest) {
        this.sfsRequest = sfsRequest;
        this.vertxContext = sfsRequest.vertxContext();
        this.authProviderService = vertxContext.verticle().authProviderService();
    }

    @Override
    public Observable<ObjectList> call(PersistentContainer container) {
        MultiMap queryParams = sfsRequest.params();
        Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();


        final String limit = queryParams.get(LIMIT);
        String marker = unescape(queryParams.get(MARKER));
        String endMarker = unescape(queryParams.get(END_MARKER));
        final String prefix = unescape(queryParams.get(PREFIX));
        final String delimiter = unescape(queryParams.get(DELIMITER));

        Integer parsedLimit = !isNullOrEmpty(limit) ? tryParse(limit) : valueOf(10000);
        parsedLimit = parsedLimit == null || parsedLimit < 0 || parsedLimit > 10000 ? 10000 : parsedLimit;

        String containerId = container.getId();

        String containerPrefix = containerId + ObjectPath.DELIMITER;
        if (!isNullOrEmpty(prefix)) {
            containerPrefix += prefix;
        }

        String objectIndex = elasticSearch.objectIndex(container.getName());

        final SearchRequestBuilder scrollRequest = elasticSearch.get()
                .prepareSearch(objectIndex)
                .setTypes(elasticSearch.defaultType())
                .addSort(DOC_FIELD_NAME, ASC)
                .setScroll(timeValueMillis(elasticSearch.getDefaultScrollTimeout()))
                .setTimeout(timeValueMillis(elasticSearch.getDefaultSearchTimeout() - 10))
                .setQuery(prefixQuery("_id", containerPrefix))
                .setSize(100);

        final Integer finalParsedLimit = parsedLimit;

        final NavigableMap<String, ListedObject> listedObjects = new TreeMap<>();
        return scan(container, prefix, delimiter, marker, endMarker, finalParsedLimit, elasticSearch, scrollRequest, listedObjects)
                .map(aVoid -> new ObjectList(container, listedObjects.values()))
                .onErrorResumeNext(throwable -> {
                    if (containsException(IndexNotFoundException.class, throwable)) {
                        return just(new ObjectList(container, emptyList()));
                    } else {
                        return error(throwable);
                    }
                });
    }

    protected Observable<Void> scan(
            final PersistentContainer container,
            final String prefix,
            final String delimiter,
            final String marker,
            final String endMarker,
            final int limit,
            final Elasticsearch elasticsearch,
            final SearchRequestBuilder scrollRequest,
            final NavigableMap<String, ListedObject> listedObjects) {

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

                    return toIterable(container, prefix, delimiter, marker, endMarker, hits)
                            .doOnNext(listedObject -> {
                                String id = listedObject.getName();
                                ListedObject existing = listedObjects.get(id);
                                if (existing == null) {
                                    listedObjects.put(id, listedObject);
                                } else {
                                    existing.setLength(existing.getLength() + listedObject.getLength());
                                }
                                if (listedObjects.size() > limit) {
                                    listedObjects.pollLastEntry();
                                }
                            })
                            .count()
                            .map(new ToVoid<>())
                            .flatMap(aVoid -> scroll(container, prefix, delimiter, marker, endMarker, limit, elasticsearch, searchResponse.getScrollId(), listedObjects));
                });
    }

    protected Observable<Void> scroll(
            final PersistentContainer container,
            final String prefix,
            final String delimiter,
            final String marker,
            final String endMarker,
            final int limit,
            final Elasticsearch elasticsearch,
            final String scrollId,
            final NavigableMap<String, ListedObject> listedObjects) {
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
                        return toIterable(container, prefix, delimiter, marker, endMarker, hits)
                                .doOnNext(listedObject -> {
                                    String id = listedObject.getName();
                                    ListedObject existing = listedObjects.get(id);
                                    if (existing == null) {
                                        listedObjects.put(id, listedObject);
                                    } else {
                                        existing.setLength(existing.getLength() + listedObject.getLength());
                                    }
                                    if (listedObjects.size() > limit) {
                                        listedObjects.pollLastEntry();
                                    }
                                })
                                .count()
                                .map(new ToVoid<>())
                                .flatMap(aVoid -> scroll(container, prefix, delimiter, marker, endMarker, limit, elasticsearch, searchResponse.getScrollId(), listedObjects));
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
                    return Defer.just(null);
                })
                .map(clearScrollResponseOptional -> null);
    }

    protected Observable<ListedObject> toIterable(final PersistentContainer container, final String prefix, final String delimiter, final String marker, final String endMarker, SearchHits searchHits) {
        // container id looks like /account/container
        // object id looks like /account/container/a/b/c/1/2/3
        // which makes the start index of the object name is the length of the
        // container id + 1
        int objectNameStartIndex = container.getId().length() + DELIMITER_LENGTH;
        return
                from(searchHits)
                        .flatMap(searchHit -> {

                            String objectId = searchHit.getId();
                            objectId = objectId.substring(objectNameStartIndex, objectId.length());
                            boolean trimmed = false;
                            if (delimiter != null) {
                                int prefixLength = prefix != null ? prefix.length() : 0;
                                int objectIdLength = objectId.length();
                                if (objectIdLength > prefixLength) {
                                    int indexOfDelimiter = objectId.indexOf(delimiter, prefixLength);
                                    if (indexOfDelimiter <= objectIdLength && indexOfDelimiter >= 0) {
                                        objectId = objectId.substring(0, indexOfDelimiter);
                                        trimmed = true;
                                    }
                                }
                            }

                            boolean isInRange = (marker == null || objectId.compareTo(marker) > 0)
                                    && (endMarker == null || objectId.compareTo(endMarker) < 0);

                            if (isInRange) {
                                PersistentObject persistentObject = fromSearchHit(container, searchHit);
                                Optional<TransientVersion> oTransientVersion = persistentObject.getNewestVersion();

                                if (oTransientVersion.isPresent()) {
                                    TransientVersion transientVersion = oTransientVersion.get();

                                    boolean finalTrimmed = trimmed;
                                    String finalObjectId = objectId;
                                    return authProviderService.canObjectRead(sfsRequest, transientVersion)
                                            .filter(canDo -> canDo)
                                            .filter(canDo -> {
                                                // TODO clean this up!. Make GET object and HEAD object share the same logic
                                                try {
                                                    new ValidateVersionNotDeleted().call(transientVersion);
                                                    new ValidateVersionNotDeleteMarker().call(transientVersion);
                                                    new ValidateVersionNotExpired().call(transientVersion);
                                                    new ValidateVersionHasSegments().call(transientVersion);
                                                    new ValidateVersionSegmentsHasData().call(transientVersion);
                                                    new ValidateVersionIsReadable().call(transientVersion);
                                                    return true;
                                                } catch (HttpRequestValidationException e) {
                                                    LOGGER.debug("Version " + transientVersion.getId() + " failed validation", e);
                                                    return false;
                                                }
                                            })
                                            .map(canDo -> {
                                                Optional<byte[]> oEtag = transientVersion.calculateMd5();
                                                Calendar lastModified = transientVersion.getUpdateTs();
                                                Optional<Long> oContentLength = transientVersion.calculateLength();
                                                Optional<String> oContentType = transientVersion.getContentType();

                                                ListedObject listedObject = new ListedObject(finalObjectId);

                                                if (oEtag.isPresent()) {
                                                    listedObject.setEtag(oEtag.get());
                                                } else {
                                                    listedObject.setEtag(EMPTY_MD5);
                                                }

                                                listedObject.setLastModified(lastModified);

                                                if (finalTrimmed) {
                                                    listedObject.setContentType("application/directory");
                                                } else if (oContentType.isPresent()) {
                                                    listedObject.setContentType(oContentType.get());
                                                } else {
                                                    listedObject.setContentType(OCTET_STREAM.toString());
                                                }

                                                if (oContentLength.isPresent()) {
                                                    listedObject.setLength(oContentLength.get());
                                                } else {
                                                    listedObject.setLength(0);
                                                }

                                                return listedObject;
                                            });


                                }
                            }
                            return empty();
                        });
    }

    public static class ListedObject {

        private byte[] etag;
        private Calendar lastModified;
        private long length;
        private String contentType;
        private final String name;

        public ListedObject(String name) {
            this.name = name;
        }

        public void setEtag(byte[] etag) {
            this.etag = etag;
        }

        public void setLastModified(Calendar lastModified) {
            this.lastModified = lastModified;
        }

        public void setLength(long length) {
            this.length = length;
        }

        public void setContentType(String contentType) {
            this.contentType = contentType;
        }

        public byte[] getEtag() {
            return etag;
        }

        public Calendar getLastModified() {
            return lastModified;
        }

        public long getLength() {
            return length;
        }

        public String getContentType() {
            return contentType;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ListedObject)) return false;

            ListedObject that = (ListedObject) o;

            return name != null ? name.equals(that.name) : that.name == null;

        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }
    }
}