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
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.hash.Hashing.md5;
import static com.google.common.hash.Hashing.sha512;
import static java.lang.Boolean.TRUE;

public abstract class Segment<T extends Segment> implements Identity {

    public static final byte[] EMPTY_MD5 = md5().hashBytes(new byte[]{}).asBytes();
    public static final byte[] EMPTY_SHA512 = sha512().hashBytes(new byte[]{}).asBytes();
    private final XVersion parent;
    private final long id;
    private Long writeLength;
    private Long readLength;
    private byte[] readMd5;
    private byte[] readSha512;
    private byte[] writeSha512;
    private byte[] tinyData;
    private Boolean isTinyData;
    private Boolean isTinyDataDeleted;
    private SegmentCipher segmentCipher;
    private List<TransientBlobReference> blobs = new ArrayList<>();

    public Segment(XVersion parent, long id) {
        this.parent = parent;
        checkState(id >= 0, "Id must be >= 0");
        this.id = id;
    }

    public XVersion<? extends XVersion> getParent() {
        return parent;
    }

    @Override
    public long getId() {
        return id;
    }

    public Optional<Long> getReadLength() {
        return fromNullable(readLength);
    }

    public T setReadLength(Long readLength) {
        this.readLength = readLength;
        return (T) this;
    }

    public Optional<byte[]> getReadMd5() {
        return fromNullable(readMd5);
    }

    public T setReadMd5(byte[] readMd5) {
        this.readMd5 = readMd5;
        return (T) this;
    }

    public Optional<byte[]> getReadSha512() {
        return fromNullable(readSha512);
    }

    public T setReadSha512(byte[] readSha512) {
        this.readSha512 = readSha512;
        return (T) this;
    }

    public Optional<byte[]> getWriteSha512() {
        return fromNullable(writeSha512);
    }

    public T setWriteSha512(byte[] writeSha512) {
        this.writeSha512 = writeSha512;
        return (T) this;
    }

    public Optional<Long> getWriteLength() {
        return fromNullable(writeLength);
    }

    public T setWriteLength(Long writeLength) {
        this.writeLength = writeLength;
        return (T) this;
    }

    public Optional<SegmentCipher> getSegmentCipher() {
        return fromNullable(segmentCipher);
    }

    public T setSegmentCipher(SegmentCipher segmentCipher) {
        this.segmentCipher = segmentCipher;
        return (T) this;
    }

    public List<TransientBlobReference> getBlobs() {
        checkState(!TRUE.equals(isTinyData), "isTinyData must be set to false");
        return blobs;
    }

    public T setBlobs(Iterable<TransientBlobReference> blobs) {
        checkState(!TRUE.equals(isTinyData), "isTinyData must be set to false");
        this.blobs.clear();
        addAll(this.blobs, blobs);
        return (T) this;
    }

    public T removeBlobs(Collection<TransientBlobReference> blobs) {
        this.blobs.removeAll(blobs);
        return (T) this;
    }

    public TransientBlobReference newBlob() {
        checkState(!TRUE.equals(isTinyData), "isTinyData must be set to false");
        TransientBlobReference blobReference = new TransientBlobReference(this);
        this.blobs.add(blobReference);
        return blobReference;
    }

    public Iterable<TransientBlobReference> verifiedPrimaryBlobs() {
        checkState(!TRUE.equals(isTinyData), "isTinyData must be set to false");
        return from(verifiedBlobs())
                .filter(TransientBlobReference::isVolumePrimary);
    }

    public Iterable<TransientBlobReference> verifiedAckdPrimaryBlobs() {
        checkState(!TRUE.equals(isTinyData), "isTinyData must be set to false");
        return from(verifiedAckdBlobs())
                .filter(TransientBlobReference::isVolumePrimary);
    }

    public Iterable<TransientBlobReference> verifiedReplicaBlobs() {
        checkState(!TRUE.equals(isTinyData), "isTinyData must be set to false");
        return from(verifiedBlobs())
                .filter(TransientBlobReference::isVolumeReplica);
    }

    public Iterable<TransientBlobReference> verifiedAckdReplicaBlobs() {
        checkState(!TRUE.equals(isTinyData), "isTinyData must be set to false");
        return from(verifiedAckdBlobs())
                .filter(TransientBlobReference::isVolumeReplica);
    }

    public Iterable<TransientBlobReference> verifiedAckdBlobs() {
        checkState(!TRUE.equals(isTinyData), "isTinyData must be set to false");
        return from(verifiedBlobs())
                .filter(BlobReference::isAcknowledged);
    }

    public Iterable<TransientBlobReference> verifiedUnAckdBlobs() {
        checkState(!TRUE.equals(isTinyData), "isTinyData must be set to false");
        return from(verifiedBlobs())
                .filter(input -> !input.isAcknowledged());
    }

    private Iterable<TransientBlobReference> verifiedBlobs() {
        checkState(!TRUE.equals(isTinyData), "isTinyData must be set to false");
        return from(blobs)
                .filter(notNull())
                .filter(blob -> {
                    Optional<String> oVolumeId = blob.getVolumeId();
                    Optional<Long> oPosition = blob.getPosition();
                    Optional<byte[]> oComputedSha512 = blob.getReadSha512();
                    Optional<Long> oComputedLength = blob.getReadLength();
                    boolean deleted = blob.isDeleted();
                    boolean hasVolumeId = oVolumeId.isPresent();
                    boolean hasPosition = oPosition.isPresent();
                    boolean sha512Match = oComputedSha512.isPresent() && Arrays.equals(writeSha512, oComputedSha512.get());
                    boolean lengthMatch = oComputedLength.isPresent() && writeLength != null && writeLength.equals(oComputedLength.get());
                    boolean noNullFields = writeLength != null && readLength != null && readMd5 != null && readSha512 != null && writeSha512 != null;
                    return !deleted && hasVolumeId && hasPosition && sha512Match && lengthMatch && noNullFields;
                });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Segment)) return false;

        Segment segment = (Segment) o;

        return id == segment.id;

    }

    public byte[] getTinyData() {
        return tinyData;
    }

    public T setTinyData(byte[] tinyData) {
        checkState(TRUE.equals(isTinyData), "isTinyData must be set to true");
        this.tinyData = tinyData;
        return (T) this;
    }

    public boolean isTinyData() {
        return TRUE.equals(isTinyData);
    }

    public T setIsTinyData(Boolean tinyData) {
        isTinyData = tinyData;
        return (T) this;
    }

    public boolean isTinyDataDeleted() {
        return TRUE.equals(isTinyDataDeleted);
    }

    public T deleteTinyData() {
        checkState(TRUE.equals(isTinyData), "isTinyData must be set to true");
        isTinyDataDeleted = true;
        return (T) this;
    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }

    public JsonObject toJsonObject() {
        JsonObject document = new JsonObject();
        document.put("id", id);
        document.put("read_md5", readMd5);
        document.put("read_sha512", readSha512);
        document.put("read_length", readLength);
        document.put("write_sha512", writeSha512);
        document.put("write_length", writeLength);
        document.put("tiny_data", tinyData);
        document.put("is_tiny_data", TRUE.equals(isTinyData));
        document.put("is_tiny_data_deleted", TRUE.equals(isTinyDataDeleted));
        if (tinyData != null) {
            checkState(TRUE.equals(isTinyData), "isTinyData must be set to true");
            checkState(blobs == null || blobs.isEmpty(), "blobs must be empty when tinyData exists");
        }
        if (blobs != null && !blobs.isEmpty()) {
            checkState(!TRUE.equals(isTinyData), "isTinyData must be set to false");
            checkState(tinyData == null, "tinyData must be empty when blobs exist");
        }

        if (segmentCipher != null) {
            document = document
                    .put("container_key_id", segmentCipher.containerKeyId)
                    .put("cipher_salt", segmentCipher.salt);
        } else {
            document.put("container_key_id", (String) null)
                    .put("cipher_salt", (byte[]) null);
        }

        JsonArray blobJsonArray = new JsonArray();
        for (TransientBlobReference transientBlobReference : blobs) {
            blobJsonArray.add(transientBlobReference.toJsonObject());
        }
        document.put("blobs", blobJsonArray);
        return document;
    }

    public T merge(JsonObject document) {
        Long id = document.getLong("id");
        checkNotNull(id, "id cannot be null");
        checkState(id == this.id, "id was %s, expected %s", id, this.id);
        setReadMd5(document.getBinary("read_md5"));
        setReadSha512(document.getBinary("read_sha512"));
        setReadLength(document.getLong("read_length"));
        setWriteSha512(document.getBinary("write_sha512"));
        setWriteLength(document.getLong("write_length"));
        isTinyData = document.getBoolean("is_tiny_data");
        tinyData = document.getBinary("tiny_data");
        isTinyDataDeleted = document.getBoolean("is_tiny_data_deleted");

        String cipherKey = document.getString("container_key_id");
        byte[] cipherSalt = document.getBinary("cipher_salt");

        segmentCipher = new SegmentCipher(cipherKey, cipherSalt);

        JsonArray blobJsonArray = document.getJsonArray("blobs");
        this.blobs.clear();
        if (blobJsonArray != null) {
            for (Object o : blobJsonArray) {
                JsonObject jsonObject = (JsonObject) o;
                TransientBlobReference transientBlobReference = new TransientBlobReference(this).merge(jsonObject);
                this.blobs.add(transientBlobReference);
            }
        }
        return (T) this;
    }

    public static class SegmentCipher {

        private String containerKeyId;
        private byte[] salt;

        public SegmentCipher(String containerKeyId, byte[] salt) {
            this.containerKeyId = containerKeyId;
            this.salt = salt;
        }

        public Optional<String> getContainerKeyId() {
            return fromNullable(containerKeyId);
        }

        public SegmentCipher setContainerKeyId(String containerKeyId) {
            this.containerKeyId = containerKeyId;
            return this;
        }

        public Optional<byte[]> getSalt() {
            return fromNullable(salt);
        }

        public SegmentCipher setSalt(byte[] salt) {
            this.salt = salt;
            return this;
        }
    }
}
