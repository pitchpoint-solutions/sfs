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

package org.sfs.vo;

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.hash.Hasher;
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.sfs.SfsRequest;
import org.sfs.metadata.Metadata;
import org.sfs.util.IdentityComparator;

import java.util.Calendar;
import java.util.Collection;
import java.util.NavigableSet;
import java.util.TreeSet;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Optional.of;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Ordering.from;
import static com.google.common.hash.Hashing.md5;
import static com.google.common.hash.Hashing.sha512;
import static com.google.common.io.BaseEncoding.base16;
import static com.google.common.io.BaseEncoding.base64;
import static com.google.common.math.LongMath.checkedAdd;
import static com.google.common.net.HttpHeaders.CONTENT_DISPOSITION;
import static com.google.common.net.HttpHeaders.CONTENT_ENCODING;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.net.HttpHeaders.CONTENT_MD5;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.ETAG;
import static com.google.common.primitives.Longs.tryParse;
import static com.google.protobuf.ByteString.copyFrom;
import static java.lang.Boolean.TRUE;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static java.util.Calendar.getInstance;
import static org.sfs.metadata.Metadata.object;
import static org.sfs.protobuf.XVolume.XDumpFile;
import static org.sfs.protobuf.XVolume.XDumpFile.Version01;
import static org.sfs.protobuf.XVolume.XDumpFile.Version01.Builder;
import static org.sfs.protobuf.XVolume.XDumpFile.Version01.newBuilder;
import static org.sfs.util.DateFormatter.fromDateTimeString;
import static org.sfs.util.DateFormatter.toDateTimeString;
import static org.sfs.util.NullSafeAscii.equalsIgnoreCase;
import static org.sfs.util.SfsHttpHeaders.X_CONTENT_SHA512;
import static org.sfs.util.SfsHttpHeaders.X_DELETE_AFTER;
import static org.sfs.util.SfsHttpHeaders.X_DELETE_AT;
import static org.sfs.util.SfsHttpHeaders.X_OBJECT_MANIFEST;
import static org.sfs.util.SfsHttpHeaders.X_SERVER_SIDE_ENCRYPTION;
import static org.sfs.vo.Segment.EMPTY_MD5;
import static org.sfs.vo.Segment.EMPTY_SHA512;

public abstract class XVersion<T extends XVersion> implements Temporal, Identity {

    private final XObject parent;
    private final long id;
    private Boolean deleteMarker;
    private Boolean deleted;
    private Metadata metadata = object();
    private Calendar createTs;
    private Calendar updateTs;
    private String contentType;
    private String contentDisposition;
    private String contentEncoding;
    private Long contentLength;
    private byte[] contentMd5;
    private byte[] contentSha512;
    private Long deleteAt;
    private byte[] etag;
    private Boolean serverSideEncryption;
    private String objectManifest;
    private Boolean staticLargeObject;
    // largest version numbers should be at the beginning of the collection
    // largest version numbers should be at the beginning of the collection
    private NavigableSet<TransientSegment> segments = new TreeSet<>(from(new IdentityComparator()).reverse());

    public XVersion(XObject parent, long id) {
        this.parent = parent;
        checkState(id >= 0, "Id must be >= 0");
        this.id = id;
    }

    @Override
    public long getId() {
        return id;
    }

    public XObject getParent() {
        return parent;
    }

    public long getAgeInMs() {
        long now = currentTimeMillis();
        long then = getUpdateTs().getTimeInMillis();
        return max(now - then, 0);
    }

    public boolean isDeleted() {
        return TRUE.equals(deleted);
    }

    public T setDeleted(Boolean deleted) {
        this.deleted = deleted;
        return (T) this;
    }

    public Optional<byte[]> getContentMd5() {
        return fromNullable(contentMd5);
    }

    public T setContentMd5(byte[] contentMd5) {
        this.contentMd5 = contentMd5;
        return (T) this;
    }

    public Optional<byte[]> getContentSha512() {
        return fromNullable(contentSha512);
    }

    public T setContentSha512(byte[] contentSha512) {
        this.contentSha512 = contentSha512;
        return (T) this;
    }

    public Boolean getDeleteMarker() {
        return deleteMarker;
    }

    public T setDeleteMarker(Boolean deleteMarker) {
        this.deleteMarker = deleteMarker;
        return (T) this;
    }

    public boolean useServerSideEncryption() {
        if (serverSideEncryption != null) {
            return serverSideEncryption;
        } else {
            PersistentContainer persistentContainer = getParent().getParent();
            return persistentContainer.getServerSideEncryption();
        }
    }

    public Optional<Boolean> getServerSideEncryption() {
        return fromNullable(serverSideEncryption);
    }

    public T setServerSideEncryption(Boolean serverSideEncryption) {
        this.serverSideEncryption = serverSideEncryption;
        return (T) this;
    }

    public Optional<Long> getContentLength() {
        return fromNullable(contentLength);
    }

    public T setContentLength(Long contentLength) {
        this.contentLength = contentLength;
        return (T) this;
    }

    public Optional<Long> getDeleteAt() {
        return fromNullable(deleteAt);
    }

    public T setDeleteAt(Long deleteAt) {
        this.deleteAt = deleteAt;
        return (T) this;
    }

    public Optional<byte[]> getEtag() {
        return fromNullable(etag);
    }

    public Optional<byte[]> calculateMd5() {
        Hasher hasher = md5().newHasher();
        int size = segments.size();
        if (segments.isEmpty() && contentLength != null && contentLength <= 0) {
            return of(EMPTY_MD5);
        } else if (size == 1) {
            return segments.first().getReadMd5();
        } else if (size >= 2) {
            for (TransientSegment transientSegment : segments) {
                hasher.putBytes(transientSegment.getReadMd5().get());
            }
            return of(hasher.hash().asBytes());
        } else {
            return absent();
        }
    }

    public Optional<byte[]> calculateSha512() {
        Hasher hasher = sha512().newHasher();
        int size = segments.size();
        if (segments.isEmpty() && contentLength != null && contentLength <= 0) {
            return of(EMPTY_SHA512);
        } else if (size == 1) {
            return segments.first().getReadSha512();
        } else if (size >= 2) {
            for (TransientSegment transientSegment : segments) {
                hasher.putBytes(transientSegment.getReadSha512().get());
            }
            return of(hasher.hash().asBytes());
        } else {
            return absent();
        }
    }

    public Optional<Long> calculateLength() {
        int size = segments.size();
        if (segments.isEmpty() && contentLength != null && contentLength <= 0) {
            return of(0L);
        } else if (size == 1) {
            return segments.first().getReadLength();
        } else if (size >= 2) {
            long sum = 0;
            for (TransientSegment transientSegment : segments) {
                sum = checkedAdd(sum, transientSegment.getReadLength().get());
            }
            return of(sum);
        } else {
            return absent();
        }
    }

    public T setEtag(byte[] etag) {
        this.etag = etag;
        return (T) this;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public Calendar getCreateTs() {
        if (createTs == null) createTs = getInstance();
        return createTs;
    }

    public T setCreateTs(Calendar createTs) {
        this.createTs = createTs;
        return (T) this;
    }

    @Override
    public Calendar getUpdateTs() {
        if (updateTs == null) updateTs = getInstance();
        return updateTs;
    }

    public T setUpdateTs(Calendar updateTs) {
        this.updateTs = updateTs;
        return (T) this;
    }

    public Optional<String> getContentType() {
        return fromNullable(contentType);
    }

    public T setContentType(String contentType) {
        this.contentType = contentType;
        return (T) this;
    }

    public T setMetadata(Metadata metadata) {
        this.metadata = metadata;
        return (T) this;
    }

    public Optional<String> getContentDisposition() {
        return fromNullable(contentDisposition);
    }

    public T setContentDisposition(String contentDisposition) {
        this.contentDisposition = contentDisposition;
        return (T) this;
    }

    public Optional<String> getContentEncoding() {
        return fromNullable(contentEncoding);
    }

    public T setContentEncoding(String contentEncoding) {
        this.contentEncoding = contentEncoding;
        return (T) this;
    }

    public Optional<String> getObjectManifest() {
        return fromNullable(objectManifest);
    }

    public T setObjectManifest(String objectManifest) {
        this.objectManifest = objectManifest;
        return (T) this;
    }

    public Optional<Boolean> getStaticLargeObject() {
        return fromNullable(staticLargeObject);
    }

    public T setStaticLargeObject(Boolean staticLargeObject) {
        this.staticLargeObject = staticLargeObject;
        return (T) this;
    }

    public T removeSegments(Collection<TransientSegment> segmentsToRemove) {
        this.segments.removeAll(segmentsToRemove);
        return (T) this;
    }

    public T clearSegments() {
        this.segments.clear();
        return (T) this;
    }

    public TransientSegment newSegment() {
        Optional<TransientSegment> oNewestSegment = getNewestSegment();
        TransientSegment newSegment;
        if (oNewestSegment.isPresent()) {
            TransientSegment newestSegment = oNewestSegment.get();
            newSegment = new TransientSegment(this, checkedAdd(newestSegment.getId(), 1));
        } else {
            newSegment = new TransientSegment(this, 0);
        }
        this.segments.add(newSegment);
        return newSegment;
    }

    public Optional<TransientSegment> getNewestSegment() {
        if (!segments.isEmpty()) {
            return of(segments.last());
        } else {
            return absent();
        }
    }

    public Optional<TransientSegment> getSegment(long id) {
        for (TransientSegment transientSegment : segments) {
            if (id == transientSegment.getId()) {
                return of(transientSegment);
            }
        }
        return absent();
    }

    public NavigableSet<TransientSegment> getSegments() {
        return segments;
    }

    public boolean isSafeToRemoveFromIndex() {
        for (TransientSegment segment : segments) {
            if (!segment.isTinyData()) {
                if (!segment.getBlobs().isEmpty()) {
                    return false;
                }
            } else {
                if (segment.getTinyData() != null) {
                    return false;
                }
            }
        }
        return true;
    }

    public Iterable<TransientSegment> readableSegments() {
        return FluentIterable.from(segments)
                .filter(notNull())
                .filter(input -> (input.isTinyData() && !input.isTinyDataDeleted()) || !isEmpty(input.verifiedAckdBlobs()));
    }

    public T setSegments(Iterable<TransientSegment> segments) {
        this.segments.clear();
        addAll(this.segments, segments);
        return (T) this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof XVersion)) return false;

        XVersion xVersion = (XVersion) o;

        return id == xVersion.id;

    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }

    public T merge(SfsRequest httpServerRequest) {

        MultiMap headers = httpServerRequest.headers();

        getMetadata().clear();
        getMetadata().withHttpHeaders(headers);

        Calendar tsNow = getInstance();

        if (createTs == null) {
            setCreateTs(tsNow);
        }

        setUpdateTs(tsNow);

        String contentEncoding = headers.get(CONTENT_ENCODING);
        String contentType = headers.get(CONTENT_TYPE);
        String contentDisposition = headers.get(CONTENT_DISPOSITION);
        String contentLength = headers.get(CONTENT_LENGTH);
        String deleteAt = headers.get(X_DELETE_AT);
        String deleteAfter = headers.get(X_DELETE_AFTER);
        String etag = headers.get(ETAG);
        String contentMd5 = headers.get(CONTENT_MD5);
        String contentSha512 = headers.get(X_CONTENT_SHA512);
        String serverSideEncryption = headers.get(X_SERVER_SIDE_ENCRYPTION);
        String objectManifest = headers.get(X_OBJECT_MANIFEST);

        if (contentLength != null) {
            Long parsed = tryParse(contentLength);
            setContentLength(parsed);
        }

        checkState(deleteAt == null || deleteAfter == null, "DeleteAt and DeleteAfter were supplied");

        if (deleteAt != null) {
            Long parsed = tryParse(deleteAt);
            setDeleteAt(parsed);
        }

        if (deleteAfter != null) {
            Long parsed = tryParse(deleteAfter);
            long now = checkedAdd(updateTs.getTimeInMillis(), parsed);
            setDeleteAt(now);
        }

        if (etag != null) {
            setEtag(base16().lowerCase().decode(etag));
        }

        if (contentMd5 != null) {
            setContentMd5(base64().decode(contentMd5));
        }

        if (contentSha512 != null) {
            setContentSha512(base64().decode(contentSha512));
        }

        setContentEncoding(contentEncoding)
                .setContentType(contentType)
                .setContentDisposition(contentDisposition)
                .setServerSideEncryption(serverSideEncryption == null ? getParent().getParent().getServerSideEncryption() : equalsIgnoreCase("true", serverSideEncryption))
                .setObjectManifest(objectManifest);

        return (T) this;
    }

    public T merge(JsonObject document) {
        setDeleted(document.getBoolean("deleted"));
        setDeleteMarker(document.getBoolean("delete_marker"));

        setContentDisposition(document.getString("content_disposition"));
        setContentType(document.getString("content_type"));
        setContentEncoding(document.getString("content_encoding"));
        setContentLength(document.getLong("content_length"));
        setEtag(document.getBinary("etag"));
        setContentMd5(document.getBinary("content_md5"));
        setContentSha512(document.getBinary("content_sha512"));
        setDeleteAt(document.getLong("delete_at"));
        setServerSideEncryption(document.getBoolean("server_side_encryption"));
        setObjectManifest(document.getString("object_manifest"));
        setStaticLargeObject(document.getBoolean("static_large_object"));

        JsonArray metadataJsonObject = document.getJsonArray("metadata", new JsonArray());
        metadata.withJsonObject(metadataJsonObject);

        this.segments.clear();
        JsonArray jsonSegments = document.getJsonArray("segments", new JsonArray());
        for (Object o : jsonSegments) {
            JsonObject segmentDocument = (JsonObject) o;
            Long segmentId = segmentDocument.getLong("id");
            checkNotNull(segmentId, "Segment id cannot be null");
            TransientSegment transientSegment =
                    new TransientSegment(this, segmentId)
                            .merge(segmentDocument);
            segments.add(transientSegment);
        }

        String createTimestamp = document.getString("create_ts");
        String updateTimestamp = document.getString("update_ts");

        if (createTimestamp != null) {
            setCreateTs(fromDateTimeString(createTimestamp));
        }
        if (updateTimestamp != null) {
            setUpdateTs(fromDateTimeString(updateTimestamp));
        }
        return (T) this;
    }

    public JsonObject toJsonObject() {
        JsonObject document = new JsonObject();
        document.put("id", id);
        document.put("deleted", deleted);
        document.put("verified", size(readableSegments()) == segments.size());
        document.put("delete_marker", deleteMarker);

        document.put("etag", etag);
        document.put("content_md5", contentMd5);
        document.put("content_sha512", contentSha512);
        document.put("content_type", contentType);
        document.put("content_encoding", contentEncoding);
        document.put("content_disposition", contentDisposition);
        document.put("content_length", contentLength);
        document.put("server_side_encryption", useServerSideEncryption());
        document.put("object_manifest", objectManifest);
        document.put("static_large_object", staticLargeObject);
        document.put("delete_at", deleteAt);

        JsonArray jsonSegments = new JsonArray();
        for (TransientSegment segment : segments) {
            JsonObject segmentDocument = segment.toJsonObject();
            jsonSegments.add(segmentDocument);
        }
        document.put("segments", jsonSegments);

        document.put("metadata", getMetadata().toJsonObject());

        document.put("create_ts", toDateTimeString(getCreateTs()));

        document.put("update_ts", toDateTimeString(getUpdateTs()));

        return document;
    }

    public T merge(Version01 exportObject) {
        XDumpFile.Metadata exportMetadata = exportObject.getMetadata();

        getMetadata().clear();
        getMetadata().withExportObject(exportMetadata);

        Calendar tsNow = getInstance();

        if (createTs == null) {
            long exportCreateTs = exportObject.getCreateTs();
            if (exportCreateTs >= 0) {
                Calendar cal = getInstance();
                cal.setTimeInMillis(exportCreateTs);
                setCreateTs(cal);
            } else {
                setCreateTs(tsNow);
            }
        }

        long exportUpdateTs = exportObject.getUpdateTs();
        if (exportUpdateTs >= 0) {
            Calendar cal = getInstance();
            cal.setTimeInMillis(exportUpdateTs);
            setUpdateTs(cal);
        } else {
            setUpdateTs(tsNow);
        }

        String contentEncoding = exportObject.getContentEncoding();
        String contentType = exportObject.getContentType();
        String contentDisposition = exportObject.getContentDisposition();
        long contentLength = exportObject.getContentLength();
        long deleteAt = exportObject.getDeleteAt();
        byte[] etag = exportObject.getEtag() != null ? exportObject.getEtag().toByteArray() : null;
        byte[] contentMd5 = exportObject.getContentMd5() != null ? exportObject.getContentMd5().toByteArray() : null;
        byte[] contentSha512 = exportObject.getContentSha512() != null ? exportObject.getContentSha512().toByteArray() : null;
        boolean serverSideEncryption = exportObject.getServerSideEncryption();
        String objectManifest = exportObject.getObjectManifest();
        boolean deleteMarker = exportObject.getDeleteMarker();
        boolean deleted = exportObject.getDeleted();

        if (contentLength >= 0) {
            setContentLength(contentLength);
        }
        if (deleteAt >= 0) {
            setDeleteAt(deleteAt);
        }

        if (etag != null && etag.length > 0) {
            setEtag(etag);
        }

        if (contentMd5 != null && contentMd5.length > 0) {
            setContentMd5(contentMd5);
        }

        if (contentSha512 != null && contentSha512.length > 0) {
            setContentSha512(contentSha512);
        }

        if (deleteMarker) {
            setDeleteMarker(true);
        }

        if (deleted) {
            setDeleted(true);
        }

        setContentEncoding(isNullOrEmpty(contentEncoding) ? null : contentEncoding)
                .setContentType(isNullOrEmpty(contentType) ? null : contentType)
                .setContentDisposition(isNullOrEmpty(contentDisposition) ? null : contentDisposition)
                .setServerSideEncryption(serverSideEncryption)
                .setObjectManifest(isNullOrEmpty(objectManifest) ? null : objectManifest);

        return (T) this;
    }

    public Version01 toExportObject() {
        Builder builder = newBuilder();

        builder.setObjectId(getParent().getId());
        builder = builder.setDeleteMarker(TRUE.equals(deleteMarker));
        if (etag != null) {
            builder = builder.setEtag(copyFrom(etag));
        }
        if (contentMd5 != null) {
            builder = builder.setContentMd5(copyFrom(contentMd5));
        }
        if (contentSha512 != null) {
            builder = builder.setContentSha512(copyFrom(contentSha512));
        }
        if (contentType != null) {
            builder = builder.setContentType(contentType);
        }
        if (contentEncoding != null) {
            builder = builder.setContentEncoding(contentEncoding);
        }
        builder = builder.setContentLength(contentLength != null ? contentLength : -1);
        builder = builder.setServerSideEncryption(TRUE.equals(serverSideEncryption));

        if (objectManifest != null) {
            builder = builder.setObjectManifest(objectManifest);
        }
        builder = builder.setStaticLargeObject(TRUE.equals(staticLargeObject));
        builder = builder.setDeleteAt(deleteAt != null ? deleteAt : -1);
        builder = builder.setCreateTs(createTs != null ? createTs.getTimeInMillis() : -1);
        builder = builder.setUpdateTs(updateTs != null ? updateTs.getTimeInMillis() : -1);
        if (metadata != null) {
            XDumpFile.Metadata exportMetadata = metadata.toExportObject();
            if (exportMetadata.getEntriesCount() > 0) {
                builder = builder.setMetadata(exportMetadata);
            }
        }
        Optional<String> oOwnerGuid = getParent().getOwnerGuid();
        if (oOwnerGuid.isPresent()) {
            builder = builder.setOwnerGuid(oOwnerGuid.get());
        }
        builder = builder.setDeleted(TRUE.equals(deleted));

        return builder.build();
    }
}
