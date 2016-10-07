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
import org.sfs.encryption.AlgorithmDef;

import java.util.Calendar;

import static com.google.common.base.Optional.fromNullable;
import static java.util.Calendar.getInstance;
import static org.sfs.encryption.AlgorithmDef.fromNameIfExists;
import static org.sfs.util.DateFormatter.fromDateTimeString;
import static org.sfs.util.DateFormatter.toDateTimeString;

public abstract class MasterKey<T extends MasterKey> {
    private final String id;
    private AlgorithmDef algorithmDef;
    private String keyId;
    private byte[] encryptedKey;
    private String backup0KeyId;
    private byte[] backup0EncryptedKey;
    private byte[] secretSalt;
    private byte[] secretSha512;
    private Calendar reEncrypteTs;
    private Calendar createTs;
    private Calendar updateTs;
    private String nodeId;

    public String getId() {
        return id;
    }

    public MasterKey(String id) {
        this.id = id;
    }

    public Optional<String> getKeyId() {
        return fromNullable(keyId);
    }

    public Optional<String> getNodeId() {
        return fromNullable(nodeId);
    }

    public T setNodeId(String nodeId) {
        this.nodeId = nodeId;
        return (T) this;
    }

    public T setKeyId(String keyId) {
        this.keyId = keyId;
        return (T) this;
    }

    public Optional<AlgorithmDef> getAlgorithmDef() {
        return fromNullable(algorithmDef);
    }

    public T setAlgorithmDef(AlgorithmDef algorithmDef) {
        this.algorithmDef = algorithmDef;
        return (T) this;
    }

    public Optional<byte[]> getEncryptedKey() {
        return fromNullable(encryptedKey);
    }

    public T setEncryptedKey(byte[] encryptedKey) {
        this.encryptedKey = encryptedKey;
        return (T) this;
    }

    public Optional<byte[]> getBackup0EncryptedKey() {
        return fromNullable(backup0EncryptedKey);
    }

    public T setBackup0EncryptedKey(byte[] backup0EncryptedKey) {
        this.backup0EncryptedKey = backup0EncryptedKey;
        return (T) this;
    }

    public Optional<String> getBackup0KeyId() {
        return fromNullable(backup0KeyId);
    }

    public T setBackup0KeyId(String backup0KeyId) {
        this.backup0KeyId = backup0KeyId;
        return (T) this;
    }

    public Optional<byte[]> getSecretSalt() {
        return fromNullable(secretSalt);
    }

    public T setSecretSalt(byte[] secretSalt) {
        this.secretSalt = secretSalt;
        return (T) this;
    }

    public Optional<byte[]> getSecretSha512() {
        return fromNullable(secretSha512);
    }

    public T setSecretSha512(byte[] secretSha512) {
        this.secretSha512 = secretSha512;
        return (T) this;
    }

    public Calendar getCreateTs() {
        if (createTs == null) createTs = getInstance();
        return createTs;
    }

    public T setCreateTs(Calendar createTs) {
        this.createTs = createTs;
        return (T) this;
    }

    public Calendar getUpdateTs() {
        if (updateTs == null) updateTs = getInstance();
        return updateTs;
    }

    public T setUpdateTs(Calendar updateTs) {
        this.updateTs = updateTs;
        return (T) this;
    }

    public Optional<Calendar> getReEncrypteTs() {
        return fromNullable(reEncrypteTs);
    }

    public T setReEncrypteTs(Calendar reEncrypteTs) {
        this.reEncrypteTs = reEncrypteTs;
        return (T) this;
    }

    public T merge(JsonObject document) {
        setAlgorithmDef(fromNameIfExists(document.getString("algorithm_name")));
        setEncryptedKey(document.getBinary("encrypted_key"));
        setBackup0EncryptedKey(document.getBinary("backup0_encrypted_key"));
        setKeyId(document.getString("key_id"));
        setBackup0KeyId(document.getString("backup0_key_id"));
        setSecretSalt(document.getBinary("secret_salt"));
        setSecretSha512(document.getBinary("secret_sha512"));

        String reEncryptTs = document.getString("re_encrypt_ts");
        if (reEncryptTs != null) {
            setReEncrypteTs(fromDateTimeString(reEncryptTs));
        }

        setNodeId(document.getString("node_id"));

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

        if (algorithmDef != null) {
            document = document.put("algorithm_name", algorithmDef.getAlgorithmName());
        } else {
            document = document.put("algorithm_name", (String) null);
        }

        document = document.put("encrypted_key", encryptedKey);
        document = document.put("backup0_encrypted_key", backup0EncryptedKey);

        document = document.put("key_id", keyId);
        document = document.put("backup0_key_id", backup0KeyId);
        document = document.put("secret_salt", secretSalt);
        document = document.put("secret_sha512", secretSha512);

        document = document.put("re_encrypt_ts", reEncrypteTs != null ? toDateTimeString(reEncrypteTs) : null);

        document = document.put("node_id", nodeId);

        document = document.put("create_ts", toDateTimeString(getCreateTs()));

        document = document.put("update_ts", toDateTimeString(getUpdateTs()));

        return document;
    }
}
