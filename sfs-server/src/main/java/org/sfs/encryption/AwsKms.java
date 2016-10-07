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

package org.sfs.encryption;


import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.services.kms.model.EncryptRequest;
import com.amazonaws.services.kms.model.ReEncryptRequest;
import com.google.common.base.Preconditions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.rx.Defer;
import rx.Observable;
import rx.functions.Func0;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class AwsKms implements Kms {

    private static final Logger LOGGER = LoggerFactory.getLogger(AwsKms.class);
    private Properties properties;
    private AWSKMSClient kms;
    private String keyId;
    private String accessKeyId;
    private String secretKey;
    private AtomicBoolean started = new AtomicBoolean(false);

    public AwsKms() {
    }

    public Observable<Void> start(VertxContext<Server> vertxContext,
                                  JsonObject config) {
        AwsKms _this = this;

        return Defer.empty()
                .filter(aVoid -> started.compareAndSet(false, true))
                .flatMap(aVoid -> {
                    String keyStoreAwsKmsEndpoint = config.getString("keystore.aws.kms.endpoint", System.getProperty("sfs.keystore.aws.kms.endpoint", System.getenv("SFS_KEYSTORE_AWS_KMS_ENDPOINT")));
                    LOGGER.info(String.format("keystore.aws.kms.endpoint: %s", keyStoreAwsKmsEndpoint));
                    Preconditions.checkArgument(keyStoreAwsKmsEndpoint != null, "keystore.aws.kms.endpoint is required");

                    _this.keyId = config.getString("keystore.aws.kms.key_id", System.getProperty("sfs.keystore.aws.kms.key_id", System.getenv("SFS_KEYSTORE_AWS_KMS_KEY_ID")));
                    LOGGER.info(String.format("keystore.aws.kms.key_id: %s", _this.keyId));
                    Preconditions.checkArgument(_this.keyId != null, "keystore.aws.kms.key_id is required");

                    _this.accessKeyId = config.getString("keystore.aws.kms.access_key_id", System.getProperty("sfs.keystore.aws.kms.access_key_id", System.getenv("SFS_KEYSTORE_AWS_KMS_ACCESS_KEY_ID")));
                    LOGGER.info(String.format("keystore.aws.kms.access_key_id: %s", _this.accessKeyId));
                    Preconditions.checkArgument(_this.accessKeyId != null, "keystore.aws.kms.access_key_id is required");

                    _this.secretKey = config.getString("keystore.aws.kms.secret_key", System.getProperty("sfs.keystore.aws.kms.secret_key", System.getenv("SFS_KEYSTORE_AWS_KMS_SECRET_KEY")));
                    LOGGER.info(String.format("keystore.aws.kms.secret_key: ****"));
                    Preconditions.checkArgument(_this.secretKey != null, "keystore.aws.kms.secret_key is required");


                    return vertxContext.executeBlocking(
                            () -> {
                                kms = new AWSKMSClient(new AWSCredentials() {
                                    @Override
                                    public String getAWSAccessKeyId() {
                                        return _this.accessKeyId;
                                    }

                                    @Override
                                    public String getAWSSecretKey() {
                                        return _this.secretKey;
                                    }
                                });
                                kms.setEndpoint(keyStoreAwsKmsEndpoint);
                                return (Void) null;
                            });
                })
                .singleOrDefault(null);
    }

    public String getKeyId() {
        return keyId;
    }

    @Override
    public Observable<Encrypted> encrypt(VertxContext<Server> vertxContext, byte[] plainBytes) {
        return Observable.defer(new Func0<Observable<Encrypted>>() {
            @Override
            public Observable<Encrypted> call() {
                byte[] cloned = Arrays.copyOf(plainBytes, plainBytes.length);
                return vertxContext.executeBlocking(() -> {
                    try {
                        EncryptRequest req =
                                new EncryptRequest()
                                        .withKeyId(keyId)
                                        .withPlaintext(ByteBuffer.wrap(cloned));
                        ByteBuffer buffer = kms.encrypt(req).getCiphertextBlob();
                        byte[] b = new byte[buffer.remaining()];
                        buffer.get(b);
                        return new Encrypted(b, String.format("xppsaws:%s", keyId));
                    } finally {
                        Arrays.fill(cloned, (byte) 0);
                    }
                });
            }
        });
    }

    @Override
    public Observable<Encrypted> reencrypt(VertxContext<Server> vertxContext, byte[] cipherBytes) {
        return Observable.defer(() -> vertxContext.executeBlocking(() -> {
            ReEncryptRequest req =
                    new ReEncryptRequest()
                            .withDestinationKeyId(keyId)
                            .withCiphertextBlob(ByteBuffer.wrap(cipherBytes.clone()));
            ByteBuffer buffer = kms.reEncrypt(req).getCiphertextBlob();
            byte[] b = new byte[buffer.remaining()];
            buffer.get(b);
            return new Encrypted(b, keyId);
        }));
    }

    @Override
    public Observable<byte[]> decrypt(VertxContext<Server> vertxContext, byte[] cipherBytes) {
        return Observable.defer(() -> vertxContext.executeBlocking(() -> {
            DecryptRequest req =
                    new DecryptRequest()
                            .withCiphertextBlob(ByteBuffer.wrap(cipherBytes.clone()));
            ByteBuffer buffer = kms.decrypt(req).getPlaintext();
            byte[] b = new byte[buffer.remaining()];
            buffer.get(b);
            return b;
        }));
    }

    public Observable<Void> stop(VertxContext<Server> vertxContext) {
        return Defer.empty()
                .filter(aVoid -> started.compareAndSet(true, false))
                .flatMap(aVoid -> {
                    if (properties != null) {
                        properties.clear();
                        properties = null;
                    }
                    if (kms != null) {
                        return vertxContext.executeBlocking(() -> {
                            try {
                                kms.shutdown();
                            } catch (Throwable e) {
                                LOGGER.warn("Unhandled Exception", e);
                            }
                            return (Void) null;
                        });
                    }
                    return Defer.empty();
                })
                .singleOrDefault(null);

    }

}
