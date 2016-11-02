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

import com.google.protobuf.InvalidProtocolBufferException;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.azure.keyvault.KeyVaultClient;
import com.microsoft.azure.keyvault.authentication.KeyVaultCredentials;
import com.microsoft.azure.keyvault.models.KeyOperationResult;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.core.pipeline.filter.ServiceRequestContext;
import com.microsoft.windowsazure.credentials.CloudCredentials;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.util.ConfigHelper;
import rx.Observable;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.protobuf.ByteString.copyFrom;
import static com.microsoft.azure.keyvault.KeyVaultClientService.create;
import static com.microsoft.azure.keyvault.KeyVaultConfiguration.configure;
import static com.microsoft.azure.keyvault.extensions.cryptography.algorithms.RsaOaep.AlgorithmName;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.lang.System.getenv;
import static java.util.Arrays.fill;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.sfs.protobuf.AzureProtoBuff.CipherText;
import static org.sfs.protobuf.AzureProtoBuff.CipherText.newBuilder;
import static org.sfs.protobuf.AzureProtoBuff.CipherText.parseFrom;
import static org.sfs.rx.Defer.aVoid;
import static rx.Observable.defer;
import static rx.Observable.using;

public class AzureKms implements Kms {

    private static final Logger LOGGER = getLogger(AzureKms.class);
    private Properties properties;
    private KeyVaultClient kms;
    private String endpoint;
    private String keyId;
    private String accessKeyId;
    private String secretKey;
    private String azureKeyIdentifier;
    private AtomicBoolean started = new AtomicBoolean(false);
    private ExecutorService executorService;

    public AzureKms() {
    }

    public Observable<Void> start(VertxContext<Server> vertxContext,
                                  JsonObject config) {

        return aVoid()
                .filter(aVoid -> started.compareAndSet(false, true))
                .flatMap(aVoid -> {
                    executorService = newCachedThreadPool();

                    endpoint = ConfigHelper.getFieldOrEnv(config, "keystore.azure.kms.endpoint");
                    checkArgument(endpoint != null, "keystore.azure.kms.endpoint is required");

                    keyId = ConfigHelper.getFieldOrEnv(config, "keystore.azure.kms.key_id");
                    checkArgument(keyId != null, "keystore.azure.kms.key_id is required");

                    accessKeyId = ConfigHelper.getFieldOrEnv(config, "keystore.azure.kms.access_key_id");
                    checkArgument(accessKeyId != null, "keystore.aws.kms.access_key_id is required");

                    secretKey = ConfigHelper.getFieldOrEnv(config, "keystore.azure.kms.secret_key");
                    checkArgument(secretKey != null, "keystore.azure.kms.secret_key is required");

                    azureKeyIdentifier = format("%s/keys/%s", endpoint, keyId);

                    return vertxContext.executeBlocking(() -> {
                        try {
                            kms = createKeyVaultClient(vertxContext);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        return (Void) null;
                    });
                })
                .singleOrDefault(null);

    }

    public Configuration createConfiguration(VertxContext<Server> vertxContext) throws Exception {
        return configure(null, createCredentials(vertxContext));
    }

    protected KeyVaultClient createKeyVaultClient(VertxContext<Server> vertxContext) throws Exception {
        Configuration config = createConfiguration(vertxContext);

        return create(config);
    }

    private CloudCredentials createCredentials(VertxContext<Server> vertxContext) throws Exception {
        return new KeyVaultCredentials() {

            @Override
            public Header doAuthenticate(ServiceRequestContext request, Map<String, String> challenge) {
                try {
                    String authorization = challenge.get("authorization");
                    String resource = challenge.get("resource");
                    AuthenticationResult authResult = getAccessToken(vertxContext, accessKeyId, secretKey, authorization, resource);
                    return new BasicHeader("Authorization", authResult.getAccessTokenType() + " " + authResult.getAccessToken());
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        };
    }

    private AuthenticationResult getAccessToken(VertxContext<Server> vertxContext, String clientId, String clientKey, String authorization, String resource) throws Exception {
        AuthenticationContext context = new AuthenticationContext(authorization, false, executorService);
        ClientCredential credentials = new ClientCredential(clientId, clientKey);
        AuthenticationResult result = context.acquireToken(resource, credentials, null).get();
        checkNotNull(result, "AuthenticationResult was null");
        return result;
    }

    public String getKeyId() {
        return keyId;
    }

    @Override
    public Observable<Encrypted> encrypt(VertxContext<Server> vertxContext, byte[] plainBytes) {
        return defer(() -> vertxContext.executeBlocking(() -> {
            String algorithm = AlgorithmName;
            Future<KeyOperationResult> encrypted = kms.encryptAsync(azureKeyIdentifier, algorithm, plainBytes);
            try {
                KeyOperationResult result = encrypted.get(60, SECONDS);
                CipherText instance =
                        newBuilder()
                                .setAlgorithm(algorithm)
                                .setKeyIdentifier(result.getKid())
                                .setData(copyFrom(result.getResult()))
                                .build();
                return new Encrypted(instance.toByteArray(), format("xppsazure:%s", azureKeyIdentifier));
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    @Override
    public Observable<Encrypted> reencrypt(VertxContext<Server> vertxContext, byte[] cipherBytes) {
        return decrypt(vertxContext, cipherBytes)
                .flatMap(clearBytes -> using(
                        () -> clearBytes,
                        bytes -> encrypt(vertxContext, bytes),
                        bytes -> fill(bytes, (byte) 0)));
    }

    @Override
    public Observable<byte[]> decrypt(VertxContext<Server> vertxContext, byte[] cipherBytes) {
        return defer(() -> vertxContext.executeBlocking(() -> {
            try {
                CipherText instance = parseFrom(cipherBytes.clone());
                String keyIdentifier = instance.getKeyIdentifier();
                String algorithm = instance.getAlgorithm();
                byte[] data = instance.getData().toByteArray();
                Future<KeyOperationResult> future = kms.decryptAsync(keyIdentifier, algorithm, data);
                KeyOperationResult result = future.get(60, SECONDS);
                return result.getResult();
            } catch (InvalidProtocolBufferException | InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    public Observable<Void> stop(VertxContext<Server> vertxContext) {
        return aVoid()
                .filter(aVoid -> started.compareAndSet(true, false))
                .flatMap(aVoid -> {
                    if (properties != null) {
                        properties.clear();
                        properties = null;
                    }
                    if (kms != null) {
                        return vertxContext.executeBlocking(() -> {
                            try {
                                kms.close();
                            } catch (Throwable e) {
                                LOGGER.warn("Unhandled Exception", e);
                            }
                            return (Void) null;
                        });
                    }
                    if (executorService != null) {
                        try {
                            executorService.shutdown();
                        } catch (Throwable e) {
                            LOGGER.warn("Unhandled Exception", e);
                        } finally {
                            executorService = null;
                        }
                    }
                    return aVoid();
                })
                .singleOrDefault(null);
    }
}
