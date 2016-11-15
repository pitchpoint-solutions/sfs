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

package org.sfs.jobs;


import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.nodes.Nodes;
import org.sfs.rx.Defer;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpStatusCodeException;
import rx.Observable;

import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static org.sfs.rx.Defer.aVoid;

public class Jobs {

    public static class ID {

        public static final String RE_ENCRYPT_CONTAINER_KEYS = "re_encrypt_container_keys";
        public static final String RE_ENCRYPT_MASTER_KEYS = "re_encrypt_master_keys";
        public static final String REPAIR_MASTER_KEYS = "repair_master_keys";
        public static final String VERIFY_REPAIR_CONTAINER_OBJECTS = "verify_repair_container_objects";
        public static final String VERIFY_REPAIR_ALL_CONTAINERS_OBJECTS = "verify_repair_all_container_objects";
        public static final String VERIFY_REPAIR_OBJECT = "verify_repair_object";
        public static final String ASSIGN_DOCUMENTS_TO_NODE = "assign_documents_to_node";
    }

    public static class Parameters {
        public static final String JOB_ID = "job_id";
        public static final String CONTAINER_ID = "container_id";
        public static final String OBJECT_ID = "object_id";
        public static final String TIMEOUT = "timeout";
        public static final String FORCE_REMOVE_VOLUMES = "force-remove-volumes";
    }

    private static final Logger LOGGER = getLogger(Jobs.class);
    private AtomicBoolean started = new AtomicBoolean(false);

    private Map<String, Class<? extends Job>> jobs = new HashMap<>();
    private ConcurrentMap<String, Job> runningJobs = new ConcurrentHashMap<>();

    public Jobs() {

        AssignDocumentsToNodeJob assignDocumentsToNodeJob = new AssignDocumentsToNodeJob();
        VerifyRepairAllContainerObjects verifyRepairAllContainerObjects = new VerifyRepairAllContainerObjects();
        VerifyRepairContainerObjects verifyRepairContainerObjects = new VerifyRepairContainerObjects();
        ReEncryptContainerKeys reEncryptContainerKeys = new ReEncryptContainerKeys();
        ReEncryptMasterKeys reEncryptMasterKeys = new ReEncryptMasterKeys();
        RepairMasterKeys repairMasterKeys = new RepairMasterKeys();

        register(assignDocumentsToNodeJob);
        register(verifyRepairAllContainerObjects);
        register(verifyRepairContainerObjects);
        register(reEncryptContainerKeys);
        register(reEncryptMasterKeys);
        register(repairMasterKeys);
    }

    public Observable<Void> open(VertxContext<Server> vertxContext, JsonObject config) {
        return aVoid()
                .filter(aVoid -> started.compareAndSet(false, true))
                .singleOrDefault(null);
    }

    public Observable<Void> stop(VertxContext<Server> vertxContext, String jobId) {
        return Defer.aVoid()
                .doOnNext(aVoid -> checkMaster(vertxContext))
                .map(aVoid -> getJob(jobId))
                .map(jobClass -> runningJobs.get(jobId))
                .flatMap(job -> {
                    if (job != null) {
                        return job.stop(vertxContext);
                    } else {
                        return Defer.aVoid();
                    }
                });
    }

    public Observable<Void> waitStopped(VertxContext<Server> vertxContext, String jobId) {
        return Defer.aVoid()
                .doOnNext(aVoid -> checkMaster(vertxContext))
                .map(aVoid -> getJob(jobId))
                .map(jobClass -> runningJobs.get(jobId))
                .flatMap(job -> {
                    if (job != null) {
                        return job.waitStopped(vertxContext);
                    } else {
                        return Defer.aVoid();
                    }
                });
    }

    public Observable<Void> waitStopped(VertxContext<Server> vertxContext, String jobId, long timeout, TimeUnit timeUnit) {
        return Defer.aVoid()
                .doOnNext(aVoid -> checkMaster(vertxContext))
                .map(aVoid -> getJob(jobId))
                .map(jobClass -> runningJobs.get(jobId))
                .flatMap(job -> {
                    if (job != null) {
                        return job.waitStopped(vertxContext, timeout, timeUnit);
                    } else {
                        return Defer.aVoid();
                    }
                });
    }

    public Observable<Void> execute(VertxContext<Server> vertxContext, String jobId, MultiMap parameters) {
        return Defer.aVoid()
                .doOnNext(aVoid -> checkMaster(vertxContext))
                .map(aVoid -> getJob(jobId))
                .map(this::newInstance)
                .flatMap(job ->
                        Observable.using(
                                () -> runningJobs.putIfAbsent(jobId, job) == null,
                                running -> {
                                    if (running) {
                                        return job.execute(vertxContext, parameters);
                                    } else {
                                        return Observable.error(new JobAlreadyRunning());
                                    }
                                },
                                running -> {
                                    if (running) {
                                        runningJobs.remove(jobId);
                                    }
                                }));
    }


    public Observable<Void> close(VertxContext<Server> vertxContext) {
        return aVoid()
                .filter(aVoid -> started.compareAndSet(true, false))
                .onErrorResumeNext(throwable -> {
                    LOGGER.warn("Handling error", throwable);
                    return Defer.aVoid();
                })
                .flatMap(aVoid ->
                        Observable.from(runningJobs.values())
                                .flatMap(job -> job.stop(vertxContext))
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.warn("Handling error", throwable);
                                    return Defer.aVoid();
                                })
                                .count()
                                .map(new ToVoid<>()))
                .singleOrDefault(null);
    }

    private void checkMaster(VertxContext<Server> vertxContext) {
        Nodes nodes = vertxContext.verticle().nodes();
        if (!nodes.isMaster()) {
            throw new JobMustRunOnMaster();
        }
    }

    private void register(Job job) {
        jobs.put(job.id(), job.getClass());
    }

    private Class<? extends Job> getJob(String jobId) {
        Class<? extends Job> job = jobs.get(jobId);
        if (job == null) {
            throw new JobNotFound();
        }

        return job;
    }

    private Job newInstance(Class<? extends Job> clazz) {
        try {
            return clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static class JobNotFound extends HttpStatusCodeException {

        public JobNotFound() {
            super(HttpURLConnection.HTTP_NOT_FOUND);
        }
    }

    public static class JobMustRunOnMaster extends HttpStatusCodeException {

        public JobMustRunOnMaster() {
            super(HttpURLConnection.HTTP_FORBIDDEN);
        }
    }

    public static class JobAlreadyRunning extends HttpStatusCodeException {

        public JobAlreadyRunning() {
            super(HttpURLConnection.HTTP_CONFLICT);
        }
    }

    public static class WaitStoppedExpired extends HttpStatusCodeException {

        public WaitStoppedExpired() {
            super(HttpURLConnection.HTTP_CONFLICT);
        }
    }

}

