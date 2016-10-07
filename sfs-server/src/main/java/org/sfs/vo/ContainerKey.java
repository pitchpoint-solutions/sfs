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
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Calendar.getInstance;
import static org.sfs.encryption.AlgorithmDef.fromNameIfExists;
import static org.sfs.util.DateFormatter.fromDateTimeString;
import static org.sfs.util.DateFormatter.toDateTimeString;

public abstract class ContainerKey<T extends ContainerKey> {

    private final PersistentContainer persistentContainer;
    private final String id;
    private AlgorithmDef algorithmDef;
    private byte[] cipherSalt;
    private String keyStoreKeyId;
    private byte[] encryptedKey;
    private Calendar reEncryptTs;
    private Calendar createTs;
    private Calendar updateTs;
    private String nodeId;

    public String getId() {
        return id;
    }

    public ContainerKey(PersistentContainer persistentContainer, ObjectPath objectPath) {
        this.persistentContainer = persistentContainer;
        this.id = objectPath.objectPath().get();
        checkArgument(persistentContainer.getId().equals(objectPath.containerPath().get()), "Invalid container name");
    }

    public ContainerKey(String id) {
        this.persistentContainer = null;
        this.id = id;
    }

    public PersistentContainer getPersistentContainer() {
        return persistentContainer;
    }

    public Optional<AlgorithmDef> getAlgorithmDef() {
        return fromNullable(algorithmDef);
    }

    public Optional<String> getNodeId() {
        return fromNullable(nodeId);
    }

    public T setNodeId(String nodeId) {
        this.nodeId = nodeId;
        return (T) this;
    }

    public T setAlgorithmDef(AlgorithmDef algorithmDef) {
        this.algorithmDef = algorithmDef;
        return (T) this;
    }

    public Optional<byte[]> getCipherSalt() {
        return fromNullable(cipherSalt);
    }

    public T setCipherSalt(byte[] cipherSalt) {
        this.cipherSalt = cipherSalt;
        return (T) this;
    }

    public Optional<String> getKeyStoreKeyId() {
        return fromNullable(keyStoreKeyId);
    }

    public T setKeyStoreKeyId(String keyStoreKeyId) {
        this.keyStoreKeyId = keyStoreKeyId;
        return (T) this;
    }

    public Optional<byte[]> getEncryptedKey() {
        return fromNullable(encryptedKey);
    }

    public T setEncryptedKey(byte[] encryptedKey) {
        this.encryptedKey = encryptedKey;
        return (T) this;
    }

    public Calendar getReEncryptTs() {
        return reEncryptTs;
    }

    public T setReEncryptTs(Calendar reEncryptTs) {
        this.reEncryptTs = reEncryptTs;
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

    public T merge(JsonObject document) {
        setAlgorithmDef(fromNameIfExists(document.getString("cipher_name")));
        setCipherSalt(document.getBinary("cipher_salt"));
        setKeyStoreKeyId(document.getString("keystore_key_id"));
        setEncryptedKey(document.getBinary("encrypted_key"));

        String reEncryptTimestamp = document.getString("re_encrypt_ts");
        if (reEncryptTimestamp != null) {
            setReEncryptTs(fromDateTimeString(reEncryptTimestamp));
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

        document =
                document.put("account_id", persistentContainer.getParent().getId())
                        .put("container_id", persistentContainer.getId());

        if (algorithmDef != null) {
            document = document.put("cipher_name", algorithmDef.getAlgorithmName());
        } else {
            document = document.put("cipher_name", (String) null);
        }

        document = document.put("cipher_salt", cipherSalt)
                .put("keystore_key_id", keyStoreKeyId)
                .put("encrypted_key", encryptedKey)
                .put("node_id", nodeId);

        document = document.put("re_encrypt_ts", reEncryptTs != null ? toDateTimeString(reEncryptTs) : null);

        document = document.put("create_ts", toDateTimeString(getCreateTs()));

        setUpdateTs(getInstance());
        document = document.put("update_ts", toDateTimeString(getUpdateTs()));

        return document;
    }
}
