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

package org.sfs.vo;

import com.google.common.base.Optional;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Optional.fromNullable;
import static java.lang.Boolean.TRUE;

public abstract class BlobReference<T extends BlobReference> {

    private final Segment<? extends Segment> segment;
    private String volumeId;
    private Long position;
    private byte[] readSha512;
    private Long readLength;
    private Boolean acknowledged;
    private Boolean deleted;
    private Integer verifyFailCount;

    public BlobReference(Segment segment) {
        this.segment = segment;
    }

    public Segment<? extends Segment> getSegment() {
        return segment;
    }

    public Optional<Integer> getVerifyFailCount() {
        return fromNullable(verifyFailCount);
    }

    public T setVerifyFailCount(Integer verifyFailCount) {
        this.verifyFailCount = verifyFailCount;
        return (T) this;
    }

    public T setVolumeId(String volumeId) {
        this.volumeId = volumeId;
        return (T) this;
    }

    public T setPosition(Long position) {
        this.position = position;
        return (T) this;
    }

    public boolean isAcknowledged() {
        return TRUE.equals(acknowledged);
    }

    public T setAcknowledged(Boolean acknowledged) {
        this.acknowledged = acknowledged;
        return (T) this;
    }

    public boolean isDeleted() {
        return TRUE.equals(deleted);
    }

    public T setDeleted(Boolean deleted) {
        this.deleted = deleted;
        return (T) this;
    }

    public T setReadLength(Long readLength) {
        this.readLength = readLength;
        return (T) this;
    }

    public Optional<Long> getReadLength() {
        return fromNullable(readLength);
    }

    public Optional<byte[]> getReadSha512() {
        return fromNullable(readSha512);
    }

    public T setReadSha512(byte[] readSha512) {
        this.readSha512 = readSha512;
        return (T) this;
    }

    public Optional<String> getVolumeId() {
        return fromNullable(volumeId);
    }

    public Optional<Long> getPosition() {
        return fromNullable(position);
    }

    public JsonObject toJsonObject() {
        JsonObject jsonObject = new JsonObject()
                .put("volume_id", volumeId)
                .put("position", position)
                .put("read_sha512", readSha512)
                .put("read_length", readLength)
                .put("acknowledged", acknowledged)
                .put("deleted", deleted)
                .put("verify_fail_count", verifyFailCount);
        return jsonObject;
    }

    public T merge(JsonObject jsonObject) {
        volumeId = jsonObject.getString("volume_id");
        position = jsonObject.getLong("position");
        readSha512 = jsonObject.getBinary("read_sha512");
        readLength = jsonObject.getLong("read_length");
        acknowledged = jsonObject.getBoolean("acknowledged");
        deleted = jsonObject.getBoolean("deleted");
        verifyFailCount = jsonObject.getInteger("verify_fail_count", 0);

        return (T) this;
    }
}
