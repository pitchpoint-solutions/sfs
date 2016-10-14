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

package org.sfs;

import com.fasterxml.jackson.core.JsonFactory;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpClient;
import org.sfs.auth.AuthProviderService;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.encryption.AwsKms;
import org.sfs.encryption.AzureKms;
import org.sfs.encryption.ContainerKeys;
import org.sfs.encryption.MasterKeys;
import org.sfs.filesystem.temp.TempDirectoryCleaner;
import org.sfs.jobs.Jobs;
import org.sfs.nodes.ClusterInfo;
import org.sfs.nodes.NodeStats;
import org.sfs.nodes.Nodes;

import java.util.concurrent.ExecutorService;

public abstract class Server extends AbstractVerticle implements SfsVerticle {

    public abstract ExecutorService getIoPool();

    public abstract ExecutorService getBackgroundPool();

    public abstract VertxContext<Server> vertxContext();

    public abstract AuthProviderService authProviderService();

    public abstract TempDirectoryCleaner tempFileFactory();

    public abstract Elasticsearch elasticsearch();

    public abstract Nodes nodes();

    public abstract Jobs jobs();

    public abstract SfsFileSystem sfsFileSystem();

    public abstract JsonFactory jsonFactory();

    public abstract AwsKms awsKms();

    public abstract AzureKms azureKms();

    public abstract MasterKeys masterKeys();

    public abstract ContainerKeys containerKeys();

    public abstract HttpClient httpClient(boolean https);

    public abstract ClusterInfo getClusterInfo();

    public abstract NodeStats getNodeStats();

    public abstract byte[] getRemoteNodeSecret();
}
