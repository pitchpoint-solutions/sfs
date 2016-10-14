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

import com.google.common.base.Optional;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.containerkey.GetNewestContainerKey;
import org.sfs.elasticsearch.containerkey.ListReEncryptableContainerKeys;
import org.sfs.elasticsearch.containerkey.LoadContainerKey;
import org.sfs.elasticsearch.containerkey.PersistContainerKey;
import org.sfs.elasticsearch.containerkey.UpdateContainerKey;
import org.sfs.rx.Holder2;
import org.sfs.rx.Holder3;
import org.sfs.rx.ToType;
import org.sfs.rx.ToVoid;
import org.sfs.vo.ObjectPath;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.PersistentContainerKey;
import org.sfs.vo.TransientContainerKey;
import org.sfs.vo.TransientServiceDef;
import rx.Observable;
import rx.Subscriber;

import java.util.Calendar;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.padStart;
import static com.google.common.math.LongMath.checkedAdd;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Long.parseLong;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.fill;
import static java.util.Calendar.getInstance;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.sfs.encryption.AlgorithmDef.getPreferred;
import static org.sfs.rx.Defer.empty;
import static org.sfs.rx.Defer.just;
import static org.sfs.vo.ObjectPath.fromPaths;
import static rx.Observable.defer;
import static rx.Observable.using;

public class ContainerKeys {

    private static final Logger LOGGER = getLogger(ContainerKeys.class);
    private static final int DEFAULT_PAD = 19;
    private static final long DEFAULT_RE_ENCRYPT_AGE = DAYS.toMillis(30);
    private static final long DEFAULT_ROTATE_AGE = DAYS.toMillis(30);
    private AtomicBoolean closed = new AtomicBoolean(true);
    private VertxContext<Server> startedVertxContext;
    private Set<Long> timerIds = new ConcurrentSkipListSet<>();


    public Observable<Void> start(VertxContext<Server> vertxContext) {
        return empty()
                .filter(aVoid -> closed.compareAndSet(true, false))
                .map(aVoid -> {
                    startedVertxContext = vertxContext;
                    return (Void) null;
                })
                .map(aVoid -> {
                    Handler<Long> handler = new Handler<Long>() {

                        Handler<Long> _this = this;

                        @Override
                        public void handle(Long event) {
                            timerIds.remove(event);
                            maintain(startedVertxContext)
                                    .subscribe(new Subscriber<Void>() {
                                        @Override
                                        public void onCompleted() {
                                            timerIds.add(startedVertxContext.vertx().setTimer(MINUTES.toMillis(1), _this));
                                        }

                                        @Override
                                        public void onError(Throwable e) {
                                            LOGGER.warn("Unhandled Exception", e);
                                            timerIds.add(startedVertxContext.vertx().setTimer(MINUTES.toMillis(1), _this));
                                        }

                                        @Override
                                        public void onNext(Void aVoid) {

                                        }
                                    });

                        }
                    };

                    timerIds.add(startedVertxContext.vertx().setTimer(MINUTES.toMillis(1), handler));
                    return (Void) null;
                })
                .singleOrDefault(null);
    }

    public Observable<Void> stop(VertxContext<Server> vertxContext) {
        return empty()
                .filter(aVoid -> closed.compareAndSet(false, true))
                .map(aVoid -> {
                    Iterator<Long> i = timerIds.iterator();
                    while (i.hasNext()) {
                        long timerId = i.next();
                        i.remove();
                        if (startedVertxContext != null) {
                            startedVertxContext.vertx().cancelTimer(timerId);
                        }
                    }
                    return (Void) null;
                })
                .singleOrDefault(null);

    }

    public Observable<KeyResponse> algorithm(VertxContext<Server> vertxContext, PersistentContainer persistentContainer, String keyId, byte[] salt) {
        return defer(() -> {
            checkOpen();
            return just(new Holder2<>(persistentContainer, keyId))
                    .flatMap(new LoadContainerKey(vertxContext))
                    .map(holder -> {
                        Optional<PersistentContainerKey> oPersistentContainerKey = holder.value2();
                        checkState(oPersistentContainerKey.isPresent(), "ContainerKey %s not found", keyId);
                        return oPersistentContainerKey.get();
                    })
                    .flatMap(persistentContainerKey -> {
                        MasterKeys masterKeys = vertxContext.verticle().masterKeys();
                        return masterKeys.decrypt(vertxContext,
                                new MasterKeys.Encrypted(
                                        persistentContainerKey.getKeyStoreKeyId().get(),
                                        persistentContainerKey.getCipherSalt().get(),
                                        persistentContainerKey.getEncryptedKey().get()))
                                .map(Optional::get)
                                .map(clearContainerKey -> {
                                    try {
                                        AlgorithmDef algorithmDef = persistentContainerKey.getAlgorithmDef().get();
                                        Algorithm algorithm = algorithmDef.create(clearContainerKey, salt);
                                        return new KeyResponse(persistentContainerKey.getId(), salt, algorithm);
                                    } finally {
                                        fill(clearContainerKey, (byte) 0);
                                    }
                                });
                    });
        });

    }

    public Observable<KeyResponse> preferredAlgorithm(VertxContext<Server> vertxContext, PersistentContainer persistentContainer) {
        return defer(() -> {
            checkOpen();
            return just(persistentContainer)
                    .flatMap(new GetNewestContainerKey(vertxContext))
                    .flatMap(persistentContainerKeyOptional -> {
                        if (persistentContainerKeyOptional.isPresent()) {
                            return rotateIfRequired(vertxContext, persistentContainerKeyOptional.get());
                        } else {
                            return newIfAbsent(vertxContext, persistentContainer);
                        }
                    })
                    .flatMap(persistentContainerKey -> {
                        MasterKeys masterKeys = vertxContext.verticle().masterKeys();
                        return masterKeys.decrypt(vertxContext,
                                new MasterKeys.Encrypted(
                                        persistentContainerKey.getKeyStoreKeyId().get(),
                                        persistentContainerKey.getCipherSalt().get(),
                                        persistentContainerKey.getEncryptedKey().get()))
                                .map(Optional::get)
                                .map(clearContainerKey -> {
                                    try {
                                        AlgorithmDef algorithmDef = persistentContainerKey.getAlgorithmDef().get();
                                        byte[] salt = algorithmDef.generateSalt();
                                        Algorithm algorithm = algorithmDef.create(clearContainerKey, salt);
                                        return new KeyResponse(persistentContainerKey.getId(), salt, algorithm);
                                    } finally {
                                        fill(clearContainerKey, (byte) 0);
                                    }
                                });
                    });
        });

    }

    protected Observable<Void> maintain(VertxContext<Server> vertxContext) {
        return defer(() -> {
            if (vertxContext.verticle().nodes().isDataNode()) {
                Calendar threshold = getInstance();
                threshold.setTimeInMillis(currentTimeMillis() - DEFAULT_RE_ENCRYPT_AGE);
                return empty()
                        .flatMap(new ListReEncryptableContainerKeys(vertxContext, vertxContext.verticle().nodes().getNodeId(), threshold))
                        .flatMap(pck -> reEncrypt(vertxContext, pck))
                        .count()
                        .map(new ToVoid<>())
                        .singleOrDefault(null);
            } else {
                return empty();
            }
        });
    }

    protected Observable<Void> reEncrypt(VertxContext<Server> vertxContext, PersistentContainerKey persistentContainerKey) {
        return defer(() -> {
            boolean isDebugEnabled = LOGGER.isDebugEnabled();
            if (isDebugEnabled) {
                LOGGER.debug("Starting reEncrypt of key " + persistentContainerKey.getId());
            }
            MasterKeys masterKeys = vertxContext.verticle().masterKeys();
            return masterKeys.decrypt(vertxContext,
                    new MasterKeys.Encrypted(
                            persistentContainerKey.getKeyStoreKeyId().get(),
                            persistentContainerKey.getCipherSalt().get(),
                            persistentContainerKey.getEncryptedKey().get()))
                    .map(Optional::get)
                    .flatMap(clearContainerKey ->
                            using(
                                    () -> clearContainerKey,
                                    bytes -> masterKeys.encrypt(vertxContext, bytes),
                                    bytes -> fill(bytes, (byte) 0))
                                    .map(encrypted -> persistentContainerKey
                                            .setCipherSalt(encrypted.getSalt())
                                            .setEncryptedKey(encrypted.getData())
                                            .setKeyStoreKeyId(encrypted.getKeyId())
                                            .setReEncryptTs(getInstance())
                                            .setUpdateTs(getInstance())))
                    .flatMap(new UpdateContainerKey(vertxContext))
                    .onErrorResumeNext(throwable -> {
                        LOGGER.warn("Failed to reEncrypt key " + persistentContainerKey.getId(), throwable);
                        return just(null);
                    })
                    .map(new ToType<>((Void) null))
                    .map(aVoid -> {
                        if (isDebugEnabled) {
                            LOGGER.debug("Finished reEncrypt of key " + persistentContainerKey.getId());
                        }
                        return (Void) null;
                    });
        });
    }

    protected Observable<PersistentContainerKey> rotateIfRequired(VertxContext<Server> vertxContext, PersistentContainerKey existingPersistentContainerKey) {
        return defer(() -> {
            Calendar createTs = existingPersistentContainerKey.getCreateTs();
            AlgorithmDef preferredAlgorithmDef = getPreferred();

            boolean shouldRotate = shouldRotate(createTs, existingPersistentContainerKey.getAlgorithmDef().get(), preferredAlgorithmDef);

            if (shouldRotate) {
                boolean isDebugEnabled = LOGGER.isDebugEnabled();

                if (isDebugEnabled) {
                    LOGGER.debug("Starting Rotate of key " + existingPersistentContainerKey.getId());
                }

                PersistentContainer persistentContainer = existingPersistentContainerKey.getPersistentContainer();

                ObjectPath objectPath = fromPaths(existingPersistentContainerKey.getId());
                String existingKey = objectPath.objectName().get();

                ObjectPath id =
                        fromPaths(
                                persistentContainer.getId(),
                                nextKey(existingKey));


                MasterKeys masterKeys = vertxContext.verticle().masterKeys();

                byte[] clearContainerSecret = preferredAlgorithmDef.generateKey();

                return using(
                        () -> clearContainerSecret,
                        bytes -> masterKeys.encrypt(vertxContext, bytes),
                        bytes -> fill(bytes, (byte) 0))
                        .map(encrypted -> {

                            TransientContainerKey containerKey =
                                    new TransientContainerKey(persistentContainer, id);

                            containerKey.setAlgorithmDef(preferredAlgorithmDef)
                                    .setCipherSalt(encrypted.getSalt())
                                    .setEncryptedKey(encrypted.getData())
                                    .setKeyStoreKeyId(encrypted.getKeyId())
                                    .setReEncryptTs(getInstance())
                                    .setCreateTs(getInstance())
                                    .setUpdateTs(getInstance());

                            return containerKey;
                        })
                        .doOnNext(transientContainerKey -> {
                            Optional<TransientServiceDef> currentMaintainerNode = vertxContext.verticle().getClusterInfo().getCurrentMaintainerNode();
                            if (currentMaintainerNode.isPresent()) {
                                transientContainerKey.setNodeId(currentMaintainerNode.get().getId());
                            }
                        })
                        .flatMap(new PersistContainerKey(vertxContext))
                        .map(Holder2::value1)
                        .map(newPersistentContainerKeyOptional -> {
                            // if this failed to persist another thread
                            // rotated the key so return the original
                            // since next time the key persisted by another
                            // thread will be used
                            if (newPersistentContainerKeyOptional.isPresent()) {
                                if (isDebugEnabled) {
                                    LOGGER.debug("Finished Rotate of key " + existingPersistentContainerKey.getId() + ". New key is " + newPersistentContainerKeyOptional.get().getId());
                                }
                                return newPersistentContainerKeyOptional.get();
                            } else {
                                if (isDebugEnabled) {
                                    LOGGER.debug("Finished Rotate of key " + existingPersistentContainerKey.getId() + ". Another thread completed the rotation");
                                }
                                return existingPersistentContainerKey;
                            }
                        });
            } else {
                return just(existingPersistentContainerKey);
            }
        });
    }

    protected Observable<PersistentContainerKey> newIfAbsent(VertxContext<Server> vertxContext, PersistentContainer persistentContainer) {

        return defer(() -> {
            AlgorithmDef preferredAlgorithmDef = getPreferred();

            boolean isDebugEnabled = LOGGER.isDebugEnabled();

            if (isDebugEnabled) {
                LOGGER.debug("Starting Create of new key");
            }


            ObjectPath id =
                    fromPaths(
                            persistentContainer.getId(),
                            firstKey());

            byte[] clearContainerSecret = preferredAlgorithmDef.generateKey();

            MasterKeys masterKeys = vertxContext.verticle().masterKeys();

            return using(
                    () -> clearContainerSecret,
                    bytes -> masterKeys.encrypt(vertxContext, bytes),
                    bytes -> fill(bytes, (byte) 0))
                    .map(encrypted -> {
                        TransientContainerKey containerKey =
                                new TransientContainerKey(persistentContainer, id);

                        containerKey.setAlgorithmDef(preferredAlgorithmDef)
                                .setCipherSalt(encrypted.getSalt())
                                .setEncryptedKey(encrypted.getData())
                                .setKeyStoreKeyId(encrypted.getKeyId())
                                .setReEncryptTs(getInstance())
                                .setCreateTs(getInstance())
                                .setUpdateTs(getInstance());

                        return containerKey;
                    })
                    .doOnNext(transientContainerKey -> {
                        Optional<TransientServiceDef> currentMaintainerNode = vertxContext.verticle().getClusterInfo().getCurrentMaintainerNode();
                        if (currentMaintainerNode.isPresent()) {
                            transientContainerKey.setNodeId(currentMaintainerNode.get().getId());
                        }
                    })
                    .flatMap(new PersistContainerKey(vertxContext))
                    .map(Holder2::value1)
                    .flatMap(newPersistentContainerKey -> {
                        if (newPersistentContainerKey.isPresent()) {
                            return just(newPersistentContainerKey.get());
                        } else {
                            return just(new Holder2<>(persistentContainer, id.objectPath().get()))
                                    .flatMap(new LoadContainerKey(vertxContext))
                                    .map(Holder3::value2)
                                    .map(Optional::get);
                        }
                    })
                    .map(newPersistentMasterKey -> {
                        if (isDebugEnabled) {
                            LOGGER.debug("Finished Create of key " + newPersistentMasterKey.getId());
                        }
                        return newPersistentMasterKey;
                    });
        });
    }

    protected boolean shouldRotate(Calendar createTs, AlgorithmDef currentAlgorithmDef, AlgorithmDef preferredAlgorithmDef) {
        return createTs.getTimeInMillis() <= currentTimeMillis() - DEFAULT_ROTATE_AGE
                || !currentAlgorithmDef.equals(preferredAlgorithmDef);
    }

    protected void checkOpen() {
        checkState(!closed.get(), "Already close");
    }

    protected String nextKey(String value) {
        return pad(valueOf(checkedAdd(parseLong(value), 1)));
    }

    protected String firstKey() {
        return pad("0");
    }

    protected String pad(String unpadded) {
        return padStart(unpadded, DEFAULT_PAD, '0');
    }

    public static class KeyResponse {
        private final String keyId;
        private final byte[] salt;
        private final Algorithm data;

        public KeyResponse(String keyId, byte[] salt, Algorithm data) {
            this.data = data;
            this.keyId = keyId;
            this.salt = salt;
        }

        public Algorithm getData() {
            return data;
        }

        public String getKeyId() {
            return keyId;
        }

        public byte[] getSalt() {
            return salt;
        }
    }

}
