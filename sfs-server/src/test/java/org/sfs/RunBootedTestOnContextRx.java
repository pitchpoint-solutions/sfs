/*
 * Copyright 2018 The Simple File Server Authors
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

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.sfs.integration.java.func.ResetForTest;
import org.sfs.integration.java.func.WaitForHealthCheck;
import org.sfs.integration.java.help.AuthorizationFactory;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpClientResponseHeaderLogger;
import rx.Observable;
import rx.functions.Func1;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static org.sfs.integration.java.help.AuthorizationFactory.httpBasic;

public class RunBootedTestOnContextRx extends RunTestOnContextRx {

    private static final Logger LOGGER = LoggerFactory.getLogger(RunBootedTestOnContextRx.class);
    private Path rootTmpDir;
    private String esClusterName;
    private String esNodeName;
    private String esHost;
    private Path tmpDir;
    private VertxContext<Server> vertxContext;
    private SfsServer sfsServer;
    private HostAndPort hostAndPort;
    private ThreadLocal<HttpClient> threadLocalHttpClient = new ThreadLocal<>();
    private ConcurrentMap<Context, HttpClient> contextHttpClientMap = new ConcurrentHashMap<>();
    private AuthorizationFactory.Producer authAdmin = httpBasic("admin", "admin");
    private static ConcurrentHashMap<Vertx, Func1<Vertx, Void>> map = new ConcurrentHashMap<>();

    public RunBootedTestOnContextRx() {
        this(new VertxOptions());
    }

    public RunBootedTestOnContextRx(VertxOptions options) {
        this(() -> Vertx.vertx(options));
    }

    public RunBootedTestOnContextRx(Supplier<Vertx> createVertx) {
        super(createVertx, (vertx, consumer) -> {
            vertx.close(event -> {
                try {
                    Func1<Vertx, Void> cleaner = map.remove(vertx);
                    if (cleaner != null) {
                        cleaner.call(vertx);
                    }
                } finally {
                    consumer.accept(null);
                }
            });
        });
    }

    protected void cleanup(Vertx vertx) {
        LOGGER.debug("Cleaning up");
        SfsServer.testCleanup(vertx);
        if (rootTmpDir != null) {
            vertx.fileSystem().deleteRecursiveBlocking(rootTmpDir.toString(), true);
        }
        threadLocalHttpClient.remove();
        contextHttpClientMap.clear();
        rootTmpDir = null;
        esClusterName = null;
        esNodeName = null;
        esHost = null;
        tmpDir = null;
        vertxContext = null;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        Statement st = new Statement() {
            @Override
            public void evaluate() throws Throwable {
                Vertx vertx = vertx();
                Preconditions.checkState(map.putIfAbsent(vertx, (vertx1) -> {
                    cleanup(vertx);
                    return null;
                }) == null);

                String clusteruuid = UUID.randomUUID().toString();
                try {
                    rootTmpDir = createTempDirectory("");
                    tmpDir = createTempDirectory(rootTmpDir, valueOf(currentTimeMillis()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                esHost = "127.0.0.1:9300";
                esClusterName = "elasticsearch";
                esNodeName = format("test-server-node-%s", clusteruuid);

                JsonObject verticleConfig;

                Buffer buffer = vertx.fileSystem().readFileBlocking(currentThread().getContextClassLoader().getResource("intgtestconfig.json").getFile());
                verticleConfig = new JsonObject(buffer.toString(UTF_8));
                verticleConfig.put("fs.home", tmpDir.toString());

                if (!verticleConfig.containsKey("elasticsearch.cluster.name")) {
                    verticleConfig.put("elasticsearch.cluster.name", esClusterName);
                }

                if (!verticleConfig.containsKey("elasticsearch.node.name")) {
                    verticleConfig.put("elasticsearch.node.name", esNodeName);
                }

                if (!verticleConfig.containsKey("elasticsearch.discovery.zen.ping.unicast.hosts")) {
                    verticleConfig.put("elasticsearch.discovery.zen.ping.unicast.hosts", new JsonArray().add(esHost));
                }

                if (!verticleConfig.containsKey("http.listen.addresses")) {
                    int freePort = findFreePort(6677, 7777);
                    verticleConfig.put("http.listen.addresses", new JsonArray().add(HostAndPort.fromParts("127.0.0.1", freePort).toString()));
                }

                hostAndPort = HostAndPort.fromString(verticleConfig.getJsonArray("http.listen.addresses").getString(0));

                ObservableFuture<String> handler = RxHelper.observableFuture();
                sfsServer = new SfsServer();

                vertx.deployVerticle(sfsServer, new DeploymentOptions().setConfig(verticleConfig), handler.toHandler());

                handler.map(new ToVoid<>())
                        .doOnNext(aVoid1 -> {
                            vertxContext = sfsServer.vertxContext();
                            checkState(vertxContext != null, "VertxContext was null on Verticle %s", sfsServer);
                        })
                        .flatMap(aVoid -> Observable.just((Void) null)
                                .flatMap(new WaitForHealthCheck(getHttpClient(), vertx)))
                        .flatMap(aVoid1 -> Observable.just((Void) null)
                                .flatMap(new ResetForTest(getHttpClient(), authAdmin)))
                        .map(new HttpClientResponseHeaderLogger())
                        .map(new ToVoid<>())
                        .count()
                        .toBlocking().first();

                base.evaluate();
            }
        };
        return super.apply(st, description);
    }

    public Path getRootTmpDir() {
        return rootTmpDir;
    }

    public String getEsClusterName() {
        return esClusterName;
    }

    public String getEsNodeName() {
        return esNodeName;
    }

    public String getEsHost() {
        return esHost;
    }

    public Path getTmpDir() {
        return tmpDir;
    }

    public HttpClient getHttpClient() {
        HttpClient httpClient = threadLocalHttpClient.get();
        if (httpClient == null) {
            HttpClientOptions httpClientOptions = new HttpClientOptions();
            httpClientOptions.setDefaultPort(hostAndPort.getPort())
                    .setDefaultHost(hostAndPort.getHostText())
                    .setMaxPoolSize(25)
                    .setConnectTimeout(1000)
                    .setKeepAlive(false);
            httpClient = vertx().createHttpClient(httpClientOptions);
            threadLocalHttpClient.set(httpClient);
        }
        return httpClient;
    }

    public VertxContext<Server> getVertxContext() {
        return vertxContext;
    }

    public SfsServer getSfsServer() {
        return sfsServer;
    }

    public AuthorizationFactory.Producer getAuthAdmin() {
        return authAdmin;
    }

    private static int findFreePort(int first, int last) {
        while (first <= last) {
            try (ServerSocket serverSocket = new ServerSocket(first)) {
                serverSocket.setReuseAddress(false);
                return first;
            } catch (IOException e) {
                first++;
                // do nothing since we're search for an open port
            }
        }
        throw new RuntimeException("Unable to find free port in range [" + first + " - " + last + "]");
    }
}
