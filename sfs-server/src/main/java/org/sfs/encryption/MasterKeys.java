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

package org.sfs.encryption;

import com.google.common.base.Optional;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.masterkey.GetNewestMasterKey;
import org.sfs.elasticsearch.masterkey.ListReEncryptableMasterKeys;
import org.sfs.elasticsearch.masterkey.LoadMasterKey;
import org.sfs.elasticsearch.masterkey.PersistMasterKey;
import org.sfs.elasticsearch.masterkey.UpdateMasterKey;
import org.sfs.rx.Holder2;
import org.sfs.rx.ToType;
import org.sfs.rx.ToVoid;
import org.sfs.util.MacDigestFactory;
import org.sfs.vo.PersistentMasterKey;
import org.sfs.vo.TransientMasterKey;
import rx.Observable;
import rx.Subscriber;

import javax.crypto.Mac;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.padStart;
import static com.google.common.math.LongMath.checkedAdd;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.fill;
import static java.util.Calendar.getInstance;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.sfs.encryption.AlgorithmDef.getPreferred;
import static org.sfs.encryption.KmsFactory.fromKeyId;
import static org.sfs.encryption.KmsFactory.newBackup0Kms;
import static org.sfs.encryption.KmsFactory.newKms;
import static org.sfs.rx.Defer.empty;
import static org.sfs.rx.Defer.just;
import static org.sfs.rx.RxHelper.combineSinglesDelayError;
import static org.sfs.util.MacDigestFactory.SHA512;
import static rx.Observable.combineLatest;
import static rx.Observable.defer;
import static rx.Observable.error;
import static rx.Observable.using;

public class MasterKeys {

    private static final Logger LOGGER = getLogger(MasterKeys.class);
    private static final byte[] EMPTY_ARRAY = new byte[]{};
    private static final long DEFAULT_ROTATE_AGE = DAYS.toMillis(30);
    private static final long DEFAULT_RE_ENCRYPT_AGE = DAYS.toMillis(30);
    private static final int DEFAULT_PAD = 19;
    private NavigableMap<String, MasterKey> cache = new ConcurrentSkipListMap<>();
    private AtomicBoolean closed = new AtomicBoolean(true);
    private VertxContext<Server> startedVertxContext;
    private Set<Long> timerIds = new ConcurrentSkipListSet<>();
    private boolean failIfNotCached = false;

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
                            expireCache();
                            timerIds.add(startedVertxContext.vertx().setTimer(SECONDS.toMillis(1), _this));
                        }
                    };

                    timerIds.add(startedVertxContext.vertx().setTimer(SECONDS.toMillis(1), handler));
                    return (Void) null;
                })
                .map(aVoid -> {
                    Handler<Long> handler = new Handler<Long>() {

                        Handler<Long> _this = this;

                        @Override
                        public void handle(Long event) {
                            timerIds.remove(event);
                            maintain(vertxContext)
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
                    cache.clear();
                    return (Void) null;
                })
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


    public void clearCache() {
        cache.clear();
    }

    public Observable<Optional<byte[]>> decrypt(VertxContext<Server> vertxContext, Encrypted encrypted) {
        return defer(() -> {
            checkOpen();
            return getExistingKey(vertxContext, encrypted.getKeyId())
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(masterKey -> {
                        SecureSecret secureSecret = masterKey.getSecureSecret();
                        byte[] clearMasterSecret = secureSecret.getClearBytes();
                        try {
                            Algorithm algorithm = masterKey.getAlgorithmDef().create(clearMasterSecret, encrypted.getSalt());
                            return algorithm.decrypt(encrypted.getData());
                        } finally {
                            fill(clearMasterSecret, (byte) 0);
                        }
                    })
                    .map(Optional::of)
                    .singleOrDefault(absent());
        });
    }

    public Observable<Encrypted> encrypt(VertxContext<Server> vertxContext, byte[] clearBytes) {
        return defer(() -> {
            checkOpen();
            return getPreferredKey(vertxContext)
                    .map(masterKey -> {
                        byte[] salt = masterKey.getAlgorithmDef().generateSalt();
                        SecureSecret secureSecret = masterKey.getSecureSecret();
                        byte[] clearMasterSecret = secureSecret.getClearBytes();
                        try {
                            Algorithm algorithm = masterKey.getAlgorithmDef().create(clearMasterSecret, salt);
                            byte[] encryptedBytes = algorithm.encrypt(clearBytes);
                            return new Encrypted(masterKey.getKeyId(), salt, encryptedBytes);
                        } finally {
                            fill(clearMasterSecret, (byte) 0);
                        }
                    });
        });
    }

    protected Observable<Holder2<PersistentMasterKey, Boolean>> tryRepair(VertxContext<Server> vertxContext, PersistentMasterKey persistentMasterKey) {

        return defer(() -> {

            boolean isDebugEnabled = LOGGER.isDebugEnabled();

            Optional<byte[]> oSecretSalt = persistentMasterKey.getSecretSalt();
            Optional<byte[]> oSecretSha512 = persistentMasterKey.getSecretSha512();

            Observable<Optional<byte[]>> oKey = decryptPrimary0(vertxContext, persistentMasterKey, false);
            Observable<Optional<byte[]>> oBackup0Key = decryptBackup0(vertxContext, persistentMasterKey, false);

            return combineSinglesDelayError(
                    oKey,
                    oBackup0Key,
                    (oPlainBytes, oBackup0PlainBytes) -> {

                        byte[] plainBytes = oPlainBytes.isPresent() ? oPlainBytes.get() : EMPTY_ARRAY;
                        byte[] backup0PlainBytes = oBackup0PlainBytes.isPresent() ? oBackup0PlainBytes.get() : EMPTY_ARRAY;

                        return new Holder2<>(plainBytes, backup0PlainBytes);

                    })
                    .flatMap(input -> {
                        byte[] plainBytes = input.value0();
                        byte[] backup0PlainBytes = input.value1();

                        return using(
                                () -> null,
                                aVoid -> {
                                    MacDigestFactory mdf = SHA512;

                                    // Compare the primary and backup to the secret hash and overwrite the one
                                    // that doesn't match the secret hash with the one that matches the
                                    // secret hash. If there is no secret hash fall through to creating
                                    // a backup from the primary.
                                    if (oSecretSalt.isPresent() && oSecretSha512.isPresent()) {
                                        byte[] secretSalt = oSecretSalt.get();
                                        byte[] expectedSecretSha512 = oSecretSha512.get();

                                        if (secretSalt.length > 0 && expectedSecretSha512.length > 0) {

                                            byte[] plainBytesSha512 = mdf.instance(secretSalt).doFinal(plainBytes);
                                            byte[] backup0PlainBytesSha512 = mdf.instance(secretSalt).doFinal(backup0PlainBytes);

                                            boolean keyOk = Arrays.equals(plainBytesSha512, expectedSecretSha512);
                                            boolean backup0KeyOk = Arrays.equals(backup0PlainBytesSha512, expectedSecretSha512);

                                            // if the key bytes equals the hash set the backup key to the key
                                            // bytes
                                            if (keyOk && !backup0KeyOk) {
                                                if (isDebugEnabled) {
                                                    LOGGER.debug(format("Creating backup of master key %s", persistentMasterKey.getId()));
                                                }

                                                Kms backup0Kms = newBackup0Kms(vertxContext);
                                                return backup0Kms.encrypt(vertxContext, plainBytes)
                                                        .map(encrypted ->
                                                                persistentMasterKey.setBackup0EncryptedKey(encrypted.getCipherText())
                                                                        .setBackup0KeyId(encrypted.getKeyId())
                                                                        .setUpdateTs(getInstance()))
                                                        .map(persistentMasterKey1 -> {
                                                            if (isDebugEnabled) {
                                                                LOGGER.debug(format("Created backup of master key %s", persistentMasterKey1.getId()));
                                                            }
                                                            return new Holder2<>(persistentMasterKey1, TRUE);
                                                        });
                                            }
                                            // if the backup key bytes equals the hash set the key to the backup key bytes
                                            else if (!keyOk && backup0KeyOk) {
                                                if (isDebugEnabled) {
                                                    LOGGER.debug(format("Restoring master key %s from backup", persistentMasterKey.getId()));
                                                }

                                                Kms kms = newKms(vertxContext);
                                                return kms.encrypt(vertxContext, backup0PlainBytes)
                                                        .map(encrypted ->
                                                                persistentMasterKey.setEncryptedKey(encrypted.getCipherText())
                                                                        .setKeyId(encrypted.getKeyId())
                                                                        .setUpdateTs(getInstance()))
                                                        .map(persistentMasterKey1 -> {
                                                            if (isDebugEnabled) {
                                                                LOGGER.debug(format("Restored master key %s from backup", persistentMasterKey.getId()));
                                                            }
                                                            return new Holder2<>(persistentMasterKey1, TRUE);
                                                        });
                                            } else if (!keyOk && !backup0KeyOk) {
                                                LOGGER.error(format("Cannot restore master key %s", persistentMasterKey.getId()));
                                                return just(new Holder2<>(persistentMasterKey, TRUE));
                                            } else {
                                                // both keys match hash so do nothing
                                                return just(new Holder2<>(persistentMasterKey, TRUE));
                                            }
                                        }
                                    }

                                    byte[] secretSalt = mdf.generateKey();

                                    // If plain bytes has a value and backup0 plain bytes doesn't, set backup0 plain bytes
                                    // to the same value as plain bytes. The practical case for
                                    // this condition is that since the backup kms service was implemented after
                                    // the code was live the keys need to be backed up to the
                                    // backup kms and the corresponding secret hash needs to be generated
                                    if (plainBytes.length > 0 && backup0PlainBytes.length <= 0) {
                                        if (isDebugEnabled) {
                                            LOGGER.debug(format("Creating backup of master key %s", persistentMasterKey.getId()));
                                        }


                                        byte[] secretSha512 = mdf.instance(secretSalt).doFinal(plainBytes);
                                        Kms backup0Kms = newBackup0Kms(vertxContext);
                                        return backup0Kms.encrypt(vertxContext, plainBytes)
                                                .map(encrypted ->
                                                        persistentMasterKey.setBackup0EncryptedKey(encrypted.getCipherText())
                                                                .setBackup0KeyId(encrypted.getKeyId())
                                                                .setSecretSalt(secretSalt)
                                                                .setSecretSha512(secretSha512)
                                                                .setUpdateTs(getInstance()))
                                                .map(persistentMasterKey1 -> {
                                                    if (isDebugEnabled) {
                                                        LOGGER.debug(format("Created backup of master key %s", persistentMasterKey1.getId()));
                                                    }
                                                    return new Holder2<>(persistentMasterKey, TRUE);
                                                });
                                    }
                                    // covering the other conditions doesn't make sense since at this point
                                    // it would not make sense to restore a master key from a backup
                                    // if there was no hash to confirm it's validity
                                    else {
                                        LOGGER.error(format("Cannot restore master key %s", persistentMasterKey.getId()));
                                        return just(new Holder2<>(persistentMasterKey, FALSE));
                                    }
                                },
                                aVoid -> {
                                    fill(plainBytes, (byte) 0);
                                    fill(backup0PlainBytes, (byte) 0);
                                });


                    });
        });

    }

    protected Observable<Optional<MasterKey>> getExistingKey(VertxContext<Server> vertxContext, String keyId) {
        return defer(() -> {
            MasterKey masterKey = cache.get(keyId);
            if (failIfNotCached) {
                checkState(masterKey != null, "Cached MasterKey not found");
            }
            if (masterKey == null) {
                return just(keyId)
                        .flatMap(new LoadMasterKey(vertxContext))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .flatMap(pmk ->
                                decryptPrimary0(vertxContext, pmk, true)
                                        .onErrorResumeNext(throwable -> {
                                            if (throwable instanceof KeyDigestMismatchException) {
                                                LOGGER.debug("Handling Key Error", throwable);
                                                return tryRepair(vertxContext, pmk)
                                                        .flatMap(result -> {
                                                            if (TRUE.equals(result.value1())) {
                                                                return just(result.value0())
                                                                        .flatMap(new UpdateMasterKey(vertxContext))
                                                                        .map(Optional::get)
                                                                        .flatMap(repairedPmk -> decryptPrimary0(vertxContext, repairedPmk, true));
                                                            } else {
                                                                // of the rotation failed generate a new key or revalidate
                                                                // don't try to repair again since if the rotation
                                                                // failed then there is something amis that can't be
                                                                // corrected here
                                                                return rotateIfRequired(vertxContext, pmk, true)
                                                                        .flatMap(repairedPmk -> decryptPrimary0(vertxContext, repairedPmk, true));
                                                            }
                                                        });
                                            }
                                            return error(throwable);
                                        })
                                        .map(Optional::get)
                                        .map(plainBytes -> {
                                            try {
                                                return new SecureSecret().setClearBytes(plainBytes);
                                            } finally {
                                                fill(plainBytes, (byte) 0);
                                            }
                                        })
                                        .map(secureSecret ->
                                                new MasterKey(
                                                        pmk.getId(),
                                                        pmk.getAlgorithmDef().get(),
                                                        secureSecret,
                                                        pmk.getReEncrypteTs().get(),
                                                        pmk.getCreateTs()))
                                        .map(calculatedMasterKey ->
                                                cache.computeIfAbsent(
                                                        pmk.getId(),
                                                        s -> calculatedMasterKey)))
                        .map(Optional::of)
                        .singleOrDefault(absent());
            } else {
                return just(of(masterKey));
            }
        });
    }

    protected Observable<MasterKey> getPreferredKey(VertxContext<Server> vertxContext) {
        return defer(() -> {
            AlgorithmDef preferredAlgorithmDef = getPreferred();

            // The newest entry will typically be the entry with the newest
            // rotated key that matches the preferred algorithm.
            // If the entry is not in the cache check/update the
            // persistent storage
            Map.Entry<String, MasterKey> lastEntry = cache.lastEntry();
            if (lastEntry != null) {
                MasterKey candidateMasterKey = lastEntry.getValue();
                if (preferredAlgorithmDef.equals(candidateMasterKey.algorithmDef)) {
                    return just(candidateMasterKey);
                }
            }
            if (failIfNotCached) {
                checkState(lastEntry == null, "Cached MasterKey not found");
            }

            return empty()
                    .flatMap(new GetNewestMasterKey(vertxContext, preferredAlgorithmDef))
                    .flatMap(persistentMasterKeyOptional -> {
                        if (persistentMasterKeyOptional.isPresent()) {
                            PersistentMasterKey persistentMasterKey = persistentMasterKeyOptional.get();
                            return just(persistentMasterKey)
                                    .flatMap(pmk -> rotateIfRequired(vertxContext, pmk, false));

                        } else {
                            return newIfAbsent(vertxContext);

                        }
                    })
                    .flatMap(pmk ->
                            decryptPrimary0(vertxContext, pmk, true)
                                    .onErrorResumeNext(throwable -> {
                                        if (throwable instanceof KeyDigestMismatchException) {
                                            LOGGER.debug("Handling Key Error", throwable);
                                            return tryRepair(vertxContext, pmk)
                                                    .flatMap(result -> {
                                                        if (TRUE.equals(result.value1())) {
                                                            return just(result.value0())
                                                                    .flatMap(new UpdateMasterKey(vertxContext))
                                                                    .map(Optional::get)
                                                                    .flatMap(repairedPmk -> decryptPrimary0(vertxContext, repairedPmk, true));
                                                        } else {
                                                            // of the rotation failed generate a new key or revalidate
                                                            // don't try to repair again since if the rotation
                                                            // failed then there is something amis that can't be
                                                            // corrected here
                                                            return rotateIfRequired(vertxContext, pmk, true)
                                                                    .flatMap(repairedPmk -> decryptPrimary0(vertxContext, repairedPmk, true));
                                                        }
                                                    });
                                        }
                                        return error(throwable);
                                    })
                                    .map(Optional::get)
                                    .map(plainBytes -> {
                                        try {
                                            return new SecureSecret().setClearBytes(plainBytes);
                                        } finally {
                                            fill(plainBytes, (byte) 0);
                                        }
                                    })
                                    .map(secureSecret ->
                                            new MasterKey(
                                                    pmk.getId(),
                                                    pmk.getAlgorithmDef().get(),
                                                    secureSecret,
                                                    pmk.getReEncrypteTs().get(),
                                                    pmk.getCreateTs()))
                                    .map(calculatedMasterKey ->
                                            cache.computeIfAbsent(
                                                    pmk.getId(),
                                                    s -> calculatedMasterKey)));
        });
    }

    protected Observable<Optional<byte[]>> decryptPrimary0(VertxContext<Server> vertxContext, PersistentMasterKey persistentMasterKey, boolean validate) {
        return defer(() -> {
            Optional<byte[]> oEncryptedKey = persistentMasterKey.getEncryptedKey();
            Optional<String> oKeyId = persistentMasterKey.getKeyId();
            if (oKeyId.isPresent() && oEncryptedKey.isPresent()) {
                byte[] encryptedKey = oEncryptedKey.get();
                Kms kms = fromKeyId(vertxContext, oKeyId.get());
                return kms.decrypt(vertxContext, encryptedKey)
                        .map(plainBytes -> {
                            if (validate) {
                                Optional<byte[]> oSecretSalt = persistentMasterKey.getSecretSalt();
                                Optional<byte[]> oSecretSha512 = persistentMasterKey.getSecretSha512();
                                if (oSecretSalt.isPresent() && oSecretSha512.isPresent()) {
                                    byte[] secretSalt = oSecretSalt.get();
                                    byte[] expectedSecretSha512 = oSecretSha512.get();
                                    Mac mac = SHA512.instance(secretSalt);
                                    byte[] computedSecretSha512 = mac.doFinal(plainBytes);
                                    if (!Arrays.equals(expectedSecretSha512, computedSecretSha512)) {
                                        throw new KeyDigestMismatchException("Computed key digest didn't match expected key digest");
                                    }
                                    return plainBytes;
                                }
                            }
                            return plainBytes;
                        })
                        .map(Optional::of);
            } else {
                return just(Optional.<byte[]>absent());
            }
        });
    }

    protected Observable<Optional<byte[]>> decryptBackup0(VertxContext<Server> vertxContext, PersistentMasterKey persistentMasterKey, boolean validate) {
        return defer(() -> {
            Optional<byte[]> oEncryptedKey = persistentMasterKey.getBackup0EncryptedKey();
            Optional<String> oKeyId = persistentMasterKey.getBackup0KeyId();
            if (oKeyId.isPresent() && oEncryptedKey.isPresent()) {
                byte[] encryptedKey = oEncryptedKey.get();
                Kms kms = fromKeyId(vertxContext, oKeyId.get());
                return kms.decrypt(vertxContext, encryptedKey)
                        .map(plainBytes -> {
                            if (validate) {
                                Optional<byte[]> oSecretSalt = persistentMasterKey.getSecretSalt();
                                Optional<byte[]> oSecretSha512 = persistentMasterKey.getSecretSha512();
                                if (oSecretSalt.isPresent() && oSecretSha512.isPresent()) {
                                    byte[] secretSalt = oSecretSalt.get();
                                    byte[] expectedSecretSha512 = oSecretSha512.get();
                                    Mac mac = SHA512.instance(secretSalt);
                                    byte[] computedSecretSha512 = mac.doFinal(plainBytes);
                                    if (!Arrays.equals(expectedSecretSha512, computedSecretSha512)) {
                                        throw new KeyDigestMismatchException("Computed key digest didn't match expected key digest");
                                    }
                                    return plainBytes;
                                }
                            }
                            return plainBytes;
                        })
                        .map(Optional::of);
            } else {
                return just(Optional.<byte[]>absent());
            }
        });
    }

    protected void expireCache() {
        boolean isDebugEnabled = LOGGER.isDebugEnabled();
        Iterator<MasterKey> iterator = cache.values().iterator();
        AlgorithmDef preferredAlgorithmDef = getPreferred();
        while (iterator.hasNext()) {
            MasterKey masterKey = iterator.next();
            if (shouldReEncrypt(masterKey.getReEncrypteTs())
                    || shouldRotate(masterKey.getCreateTs(), masterKey.getAlgorithmDef(), preferredAlgorithmDef)) {
                if (isDebugEnabled) {
                    LOGGER.debug("Expiring key " + masterKey.getKeyId());
                }
                iterator.remove();
            }
        }
    }

    public boolean isFailIfNotCached() {
        return failIfNotCached;
    }

    public MasterKeys setFailIfNotCached(boolean failIfNotCached) {
        this.failIfNotCached = failIfNotCached;
        return this;
    }

    protected int cacheSize() {
        return cache.size();
    }

    protected boolean shouldReEncrypt(Calendar oReEncryptTs) {
        return oReEncryptTs.getTimeInMillis() <= currentTimeMillis() - DEFAULT_RE_ENCRYPT_AGE;
    }

    protected boolean shouldRotate(Calendar createTs, AlgorithmDef currentAlgorithmDef, AlgorithmDef preferredAlgorithmDef) {
        return createTs.getTimeInMillis() <= currentTimeMillis() - DEFAULT_ROTATE_AGE
                || !currentAlgorithmDef.equals(preferredAlgorithmDef);
    }

    protected Observable<PersistentMasterKey> reEncrypt(VertxContext<Server> vertxContext, PersistentMasterKey persistentMasterKey) {
        return defer(() -> {
            boolean isDebugEnabled = LOGGER.isDebugEnabled();
            if (isDebugEnabled) {
                LOGGER.debug("Starting ReEncrypt of key " + persistentMasterKey.getId());
            }

            Kms kms = fromKeyId(vertxContext, persistentMasterKey.getKeyId().get());
            Kms backup0Kms = fromKeyId(vertxContext, persistentMasterKey.getBackup0KeyId().get());

            byte[] encryptedKey = persistentMasterKey.getEncryptedKey().get();
            byte[] backup0EncryptedKey = persistentMasterKey.getBackup0EncryptedKey().get();

            Calendar now = getInstance();

            return combineSinglesDelayError(
                    kms.reencrypt(vertxContext, encryptedKey),
                    backup0Kms.reencrypt(vertxContext, backup0EncryptedKey),
                    (awsEncrypted, azureEncrypted) -> {
                        persistentMasterKey.setUpdateTs(now)
                                .setKeyId(awsEncrypted.getKeyId())
                                .setBackup0KeyId(azureEncrypted.getKeyId())
                                .setReEncrypteTs(now)
                                .setEncryptedKey(awsEncrypted.getCipherText())
                                .setBackup0EncryptedKey(azureEncrypted.getCipherText());
                        return persistentMasterKey;
                    })
                    .flatMap(new UpdateMasterKey(vertxContext))
                    .onErrorResumeNext(throwable -> {
                        LOGGER.error("Failed to update key " + persistentMasterKey.getId(), throwable);
                        return just(absent());
                    })
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .singleOrDefault(persistentMasterKey)
                    .map(updatePersistentMasterKey -> {
                        if (isDebugEnabled) {
                            LOGGER.debug("Finished ReEncrypt of key " + updatePersistentMasterKey.getId());
                        }
                        return updatePersistentMasterKey;
                    });
        });
    }

    protected Observable<PersistentMasterKey> rotateIfRequired(VertxContext<Server> vertxContext, PersistentMasterKey existingPersistentMasterKey, boolean force) {
        return defer(() -> {
            Calendar createTs = existingPersistentMasterKey.getCreateTs();
            AlgorithmDef preferredAlgorithmDef = getPreferred();

            boolean shouldRotate = force || shouldRotate(createTs, existingPersistentMasterKey.getAlgorithmDef().get(), preferredAlgorithmDef);

            if (shouldRotate) {
                boolean isDebugEnabled = LOGGER.isDebugEnabled();

                if (isDebugEnabled) {
                    LOGGER.debug("Starting Rotate of key " + existingPersistentMasterKey.getId());
                }

                Kms kms = newKms(vertxContext);
                Kms backup0Kms = newBackup0Kms(vertxContext);

                String id = nextKey(existingPersistentMasterKey.getId());

                byte[] clearMasterSecret = preferredAlgorithmDef.generateKey();

                return using(
                        () -> null,
                        aVoid -> {
                            MacDigestFactory mdf = SHA512;
                            byte[] secretSalt = mdf.generateKey();
                            byte[] secretSha512 = mdf.instance(secretSalt).doFinal(clearMasterSecret);
                            return combineLatest(
                                    kms.encrypt(vertxContext, clearMasterSecret),
                                    backup0Kms.encrypt(vertxContext, clearMasterSecret),
                                    vertxContext.verticle().nodes().getMaintainerNode(vertxContext),
                                    (awsEncrypted, azureEncrypted, persistentServiceDefOptional) -> {
                                        Calendar now = getInstance();
                                        TransientMasterKey transientMasterKey =
                                                new TransientMasterKey(id)
                                                        .setEncryptedKey(awsEncrypted.getCipherText())
                                                        .setBackup0EncryptedKey(azureEncrypted.getCipherText())
                                                        .setKeyId(awsEncrypted.getKeyId())
                                                        .setBackup0KeyId(azureEncrypted.getKeyId())
                                                        .setSecretSalt(secretSalt)
                                                        .setSecretSha512(secretSha512)
                                                        .setAlgorithmDef(preferredAlgorithmDef)
                                                        .setReEncrypteTs(now)
                                                        .setCreateTs(now)
                                                        .setUpdateTs(now);
                                        if (persistentServiceDefOptional.isPresent()) {
                                            transientMasterKey.setNodeId(persistentServiceDefOptional.get().getId());
                                        }
                                        return transientMasterKey;

                                    });
                        },
                        aVoid -> fill(clearMasterSecret, (byte) 0))
                        .flatMap(new PersistMasterKey(vertxContext))
                        .map(newPersistentMasterKey -> {
                            if (newPersistentMasterKey.isPresent()) {
                                if (isDebugEnabled) {
                                    LOGGER.debug("Finished Rotate of key " + existingPersistentMasterKey.getId() + ". New key is " + newPersistentMasterKey.get().getId());
                                }
                                return newPersistentMasterKey.get();
                            } else {
                                if (isDebugEnabled) {
                                    LOGGER.debug("Finished Rotate of key " + existingPersistentMasterKey.getId() + ". Another thread completed the rotation");
                                }
                                return existingPersistentMasterKey;
                            }
                        });

            } else {
                return just(existingPersistentMasterKey);
            }
        });
    }

    protected Observable<PersistentMasterKey> newIfAbsent(VertxContext<Server> vertxContext) {

        return defer(() -> {

            AlgorithmDef preferredAlgorithmDef = getPreferred();

            boolean isDebugEnabled = LOGGER.isDebugEnabled();

            if (isDebugEnabled) {
                LOGGER.debug("Starting Create of new key");
            }

            Kms kms = newKms(vertxContext);
            Kms backup0Kms = newBackup0Kms(vertxContext);

            String id = firstKey();

            return using(
                    preferredAlgorithmDef::generateKey,
                    clearMasterSecret -> {
                        MacDigestFactory mdf = SHA512;
                        byte[] secretSalt = mdf.generateKey();
                        byte[] secretSha512 = mdf.instance(secretSalt).doFinal(clearMasterSecret);

                        return combineLatest(
                                kms.encrypt(vertxContext, clearMasterSecret),
                                backup0Kms.encrypt(vertxContext, clearMasterSecret),
                                vertxContext.verticle().nodes().getMaintainerNode(vertxContext),
                                (awsEncrypted, azureEncrypted, persistentServiceDefOptional) -> {
                                    Calendar now = getInstance();
                                    TransientMasterKey transientMasterKey =
                                            new TransientMasterKey(id)
                                                    .setEncryptedKey(awsEncrypted.getCipherText())
                                                    .setBackup0EncryptedKey(azureEncrypted.getCipherText())
                                                    .setKeyId(awsEncrypted.getKeyId())
                                                    .setBackup0KeyId(azureEncrypted.getKeyId())
                                                    .setSecretSalt(secretSalt)
                                                    .setSecretSha512(secretSha512)
                                                    .setAlgorithmDef(preferredAlgorithmDef)
                                                    .setReEncrypteTs(now)
                                                    .setCreateTs(now)
                                                    .setUpdateTs(now);
                                    if (persistentServiceDefOptional.isPresent()) {
                                        transientMasterKey.setNodeId(persistentServiceDefOptional.get().getId());
                                    }
                                    return transientMasterKey;

                                });
                    },
                    clearMasterSecret -> fill(clearMasterSecret, (byte) 0))
                    .flatMap(new PersistMasterKey(vertxContext))
                    .flatMap(newPersistentMasterKey -> {
                        if (newPersistentMasterKey.isPresent()) {
                            return just(newPersistentMasterKey.get());
                        } else {
                            return just(id)
                                    .flatMap(new LoadMasterKey(vertxContext))
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

    protected Observable<Void> maintain(VertxContext<Server> vertxContext) {
        return defer(() -> {
            if (vertxContext.verticle().nodes().isDataNode()) {
                Calendar threshold = getInstance();
                threshold.setTimeInMillis(currentTimeMillis() - DEFAULT_RE_ENCRYPT_AGE);
                return empty()
                        .flatMap(new ListReEncryptableMasterKeys(vertxContext, vertxContext.verticle().nodes().getNodeId(), threshold))
                        .flatMap(pmk -> reEncrypt(vertxContext, pmk))
                        .map(new ToType<>((Void) null))
                        .count()
                        .map(new ToVoid<>())
                        .singleOrDefault(null);
            } else {
                return empty();
            }
        });
    }

    protected void checkClosed() {
        checkState(closed.get(), "Already open");
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

    public static class Encrypted {
        private final String keyId;
        private final byte[] salt;
        private final byte[] data;

        public Encrypted(String keyId, byte[] salt, byte[] data) {
            this.data = data;
            this.keyId = keyId;
            this.salt = salt;
        }

        public byte[] getData() {
            return data;
        }

        public String getKeyId() {
            return keyId;
        }

        public byte[] getSalt() {
            return salt;
        }
    }

    public static class MasterKey {

        private final String keyId;
        private final AlgorithmDef algorithmDef;
        private final SecureSecret secureSecret;
        private Calendar reEncrypteTs;
        private Calendar createTs;

        public MasterKey(String keyId, AlgorithmDef algorithmDef, SecureSecret secureSecret, Calendar reEncrypteTs, Calendar createTs) {
            this.keyId = keyId;
            this.algorithmDef = algorithmDef;
            this.secureSecret = secureSecret;
            this.reEncrypteTs = reEncrypteTs;
            this.createTs = createTs;
        }

        public Calendar getCreateTs() {
            return createTs;
        }

        public Calendar getReEncrypteTs() {
            return reEncrypteTs;
        }

        public MasterKey setCreateTs(Calendar createTs) {
            this.createTs = createTs;
            return this;
        }

        public MasterKey setReEncrypteTs(Calendar reEncrypteTs) {
            this.reEncrypteTs = reEncrypteTs;
            return this;
        }

        public String getKeyId() {
            return keyId;
        }

        public AlgorithmDef getAlgorithmDef() {
            return algorithmDef;
        }

        public SecureSecret getSecureSecret() {
            return secureSecret;
        }

        public Algorithm algorithm(VertxContext<Server> vertxContext, byte[] salt) {
            byte[] clearMasterKey = secureSecret.getClearBytes();
            try {
                return algorithmDef.create(clearMasterKey, salt);
            } finally {
                fill(clearMasterKey, (byte) 0);
            }
        }
    }
}
