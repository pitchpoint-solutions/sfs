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

package org.sfs.nodes;

import io.vertx.core.MultiMap;
import rx.Observable;

import java.util.concurrent.TimeUnit;

public interface MasterNode {

    Observable<Void> executeJob(String jobId, MultiMap params, long timeout, TimeUnit timeUnit);

    Observable<Void> waitForJob(String jobId, long timeout, TimeUnit timeUnit);

    Observable<Void> stopJob(String jobId, long timeout, TimeUnit timeUnit);
}
