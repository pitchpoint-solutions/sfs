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

package org.sfs;


import com.google.common.base.Preconditions;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpFrame;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerFileUpload;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.SocketAddress;
import org.sfs.auth.UserAndRole;
import org.sfs.rx.ResultMemoizeHandler;
import org.sfs.util.KeepAliveHttpServerResponse;
import rx.Observable;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SfsRequest implements HttpServerRequest {

    private final HttpServerRequest httpServerRequest;
    private final VertxContext<Server> vertxContext;
    private Map<String, Object> context;
    private boolean wroteData = false;
    private KeepAliveHttpServerResponse keepAliveHttpServerResponse;
    private ResponseWrapper responseWrapper;
    private UserAndRole userAndRole;

    public SfsRequest(VertxContext<Server> vertxContext, HttpServerRequest httpServerRequest) {
        this.vertxContext = vertxContext;
        this.httpServerRequest = httpServerRequest;
        this.responseWrapper = new ResponseWrapper(this);
    }

    public SfsRequest startProxyKeepAlive(long timeout, TimeUnit timeUnit) {
        Preconditions.checkState(!wroteData, "HttpServerResponse has already been used");
        if (keepAliveHttpServerResponse == null) {
            keepAliveHttpServerResponse = new KeepAliveHttpServerResponse(vertxContext(), timeout, timeUnit, httpServerRequest.response());
        }
        return this;
    }

    public Observable<Void> stopKeepAlive() {
        if (keepAliveHttpServerResponse != null) {
            ResultMemoizeHandler<Void> handler = new ResultMemoizeHandler<>();
            keepAliveHttpServerResponse.stopKeepAlive(handler);
            return Observable.create(handler.subscribe);
        } else {
            return Observable.just(null);
        }
    }

    public UserAndRole getUserAndRole() {
        return userAndRole;
    }

    public void setUserAndRole(UserAndRole userAndRole) {
        this.userAndRole = userAndRole;
    }

    public boolean proxyKeepAliveStarted() {
        return keepAliveHttpServerResponse != null;
    }

    public VertxContext<Server> vertxContext() {
        return vertxContext;
    }

    public Map<String, Object> context() {
        if (context == null) {
            context = new HashMap<>();
        }
        return context;
    }

    @Override
    public HttpVersion version() {
        return httpServerRequest.version();
    }

    @Override
    public HttpMethod method() {
        return httpServerRequest.method();
    }

    @Override
    public String uri() {
        return httpServerRequest.uri();
    }

    @Override
    public String path() {
        return httpServerRequest.path();
    }

    @Override
    public String query() {
        return httpServerRequest.query();
    }

    @Override
    public HttpServerResponse response() {
        if (keepAliveHttpServerResponse != null) {
            return keepAliveHttpServerResponse;
        }
        return responseWrapper;
    }

    @Override
    public MultiMap headers() {
        return httpServerRequest.headers();
    }

    @Override
    public MultiMap params() {
        return httpServerRequest.params();
    }

    @Override
    public SocketAddress remoteAddress() {
        return httpServerRequest.remoteAddress();
    }

    @Override
    public SocketAddress localAddress() {
        return httpServerRequest.localAddress();
    }

    @Override
    public X509Certificate[] peerCertificateChain() throws SSLPeerUnverifiedException {
        return httpServerRequest.peerCertificateChain();
    }

    @Override
    public String absoluteURI() {
        return httpServerRequest.absoluteURI();
    }

    @Override
    public SfsRequest bodyHandler(Handler<Buffer> bodyHandler) {
        httpServerRequest.bodyHandler(bodyHandler);
        return this;
    }

    @Override
    public NetSocket netSocket() {
        return httpServerRequest.netSocket();
    }

    @Override
    public SfsRequest setExpectMultipart(boolean expect) {
        httpServerRequest.setExpectMultipart(expect);
        return this;
    }

    @Override
    public SfsRequest uploadHandler(Handler<HttpServerFileUpload> uploadHandler) {
        httpServerRequest.uploadHandler(uploadHandler);
        return this;
    }

    @Override
    public MultiMap formAttributes() {
        return httpServerRequest.formAttributes();
    }

    @Override
    public SfsRequest endHandler(Handler<Void> endHandler) {
        httpServerRequest.endHandler(endHandler);
        return this;
    }

    @Override
    public SfsRequest handler(Handler<Buffer> handler) {
        httpServerRequest.handler(handler);
        return this;
    }

    @Override
    public SfsRequest pause() {
        httpServerRequest.pause();
        return this;
    }

    @Override
    public SfsRequest resume() {
        httpServerRequest.resume();
        return this;
    }

    @Override
    public SfsRequest exceptionHandler(Handler<Throwable> handler) {
        httpServerRequest.exceptionHandler(handler);
        return this;
    }

    @Override
    public String rawMethod() {
        return httpServerRequest.rawMethod();
    }

    @Override
    public boolean isSSL() {
        return httpServerRequest.isSSL();
    }

    @Override
    public String scheme() {
        return httpServerRequest.scheme();
    }

    @Override
    public String host() {
        return httpServerRequest.host();
    }

    @Override
    public String getHeader(String headerName) {
        return httpServerRequest.getHeader(headerName);
    }

    @Override
    public String getHeader(CharSequence headerName) {
        return httpServerRequest.getHeader(headerName);
    }

    @Override
    public String getParam(String paramName) {
        return httpServerRequest.getParam(paramName);
    }

    @Override
    public boolean isExpectMultipart() {
        return httpServerRequest.isExpectMultipart();
    }

    @Override
    public String getFormAttribute(String attributeName) {
        return httpServerRequest.getFormAttribute(attributeName);
    }

    @Override
    public ServerWebSocket upgrade() {
        return httpServerRequest.upgrade();
    }

    @Override
    public boolean isEnded() {
        return httpServerRequest.isEnded();
    }

    @Override
    public HttpServerRequest customFrameHandler(Handler<HttpFrame> handler) {
        return httpServerRequest.customFrameHandler(handler);
    }

    @Override
    public HttpConnection connection() {
        return httpServerRequest.connection();
    }

    private static class ResponseWrapper implements HttpServerResponse {

        private final SfsRequest httpServerRequest;
        private final HttpServerResponse response;

        public ResponseWrapper(SfsRequest request) {
            this.httpServerRequest = request;
            this.response = request.httpServerRequest.response();
        }

        @Override
        public int getStatusCode() {
            return response.getStatusCode();
        }

        @Override
        public HttpServerResponse setStatusCode(int statusCode) {
            return response.setStatusCode(statusCode);
        }

        @Override
        public String getStatusMessage() {
            return response.getStatusMessage();
        }

        @Override
        public HttpServerResponse setStatusMessage(String statusMessage) {
            response.setStatusMessage(statusMessage);
            return this;
        }

        @Override
        public HttpServerResponse setChunked(boolean chunked) {
            response.setChunked(chunked);
            return this;
        }

        @Override
        public boolean isChunked() {
            return response.isChunked();
        }

        @Override
        public MultiMap headers() {
            return response.headers();
        }

        @Override
        public HttpServerResponse putHeader(String name, String value) {
            response.putHeader(name, value);
            return this;
        }

        @Override
        public HttpServerResponse putHeader(CharSequence name, CharSequence value) {
            response.putHeader(name, value);
            return this;
        }

        @Override
        public HttpServerResponse putHeader(String name, Iterable<String> values) {
            response.putHeader(name, values);
            return this;
        }

        @Override
        public HttpServerResponse putHeader(CharSequence name, Iterable<CharSequence> values) {
            response.putHeader(name, values);
            return this;
        }

        @Override
        public MultiMap trailers() {
            return response.trailers();
        }

        @Override
        public HttpServerResponse putTrailer(String name, String value) {
            response.putTrailer(name, value);
            return this;
        }

        @Override
        public HttpServerResponse putTrailer(CharSequence name, CharSequence value) {
            response.putTrailer(name, value);
            return this;
        }

        @Override
        public HttpServerResponse putTrailer(String name, Iterable<String> values) {
            response.putTrailer(name, values);
            return this;
        }

        @Override
        public HttpServerResponse putTrailer(CharSequence name, Iterable<CharSequence> value) {
            response.putTrailer(name, value);
            return this;
        }

        @Override
        public HttpServerResponse closeHandler(Handler<Void> handler) {
            response.closeHandler(handler);
            return this;
        }

        @Override
        public HttpServerResponse write(Buffer chunk) {
            httpServerRequest.wroteData = true;
            response.write(chunk);
            return this;
        }

        @Override
        public HttpServerResponse write(String chunk, String enc) {
            httpServerRequest.wroteData = true;
            response.write(chunk, enc);
            return this;
        }

        @Override
        public void end(String chunk) {
            httpServerRequest.wroteData = true;
            response.end(chunk);
        }

        @Override
        public HttpServerResponse write(String chunk) {
            httpServerRequest.wroteData = true;
            response.write(chunk);
            return this;
        }

        @Override
        public void end(String chunk, String enc) {
            httpServerRequest.wroteData = true;
            response.end(chunk, enc);
        }

        @Override
        public void end(Buffer chunk) {
            httpServerRequest.wroteData = true;
            response.end(chunk);
        }

        @Override
        public void end() {
            httpServerRequest.wroteData = true;
            response.end();
        }

        @Override
        public HttpServerResponse sendFile(String filename) {
            httpServerRequest.wroteData = true;
            response.sendFile(filename);
            return this;
        }

        @Override
        public HttpServerResponse sendFile(String filename, long offset) {
            httpServerRequest.wroteData = true;
            response.sendFile(filename, offset);
            return this;
        }

        @Override
        public HttpServerResponse sendFile(String filename, long offset, long length) {
            httpServerRequest.wroteData = true;
            response.sendFile(filename, offset, length);
            return this;
        }

        @Override
        public HttpServerResponse sendFile(String filename, Handler<AsyncResult<Void>> resultHandler) {
            httpServerRequest.wroteData = true;
            response.sendFile(filename, resultHandler);
            return this;
        }

        @Override
        public HttpServerResponse sendFile(String filename, long offset, Handler<AsyncResult<Void>> resultHandler) {
            httpServerRequest.wroteData = true;
            response.sendFile(filename, offset, resultHandler);
            return this;
        }

        @Override
        public HttpServerResponse sendFile(String filename, long offset, long length, Handler<AsyncResult<Void>> resultHandler) {
            httpServerRequest.wroteData = true;
            response.sendFile(filename, offset, length, resultHandler);
            return this;
        }

        @Override
        public void close() {
            response.close();
        }

        @Override
        public HttpServerResponse exceptionHandler(Handler<Throwable> handler) {
            response.exceptionHandler(handler);
            return this;
        }

        @Override
        public HttpServerResponse setWriteQueueMaxSize(int maxSize) {
            return response.setWriteQueueMaxSize(maxSize);
        }

        @Override
        public boolean writeQueueFull() {
            return response.writeQueueFull();
        }

        @Override
        public HttpServerResponse drainHandler(Handler<Void> handler) {
            response.drainHandler(handler);
            return this;
        }

        @Override
        public HttpServerResponse writeContinue() {
            httpServerRequest.wroteData = true;
            response.writeContinue();
            return this;
        }

        @Override
        public boolean ended() {
            return response.ended();
        }

        @Override
        public boolean closed() {
            return response.closed();
        }

        @Override
        public boolean headWritten() {
            return response.headWritten();
        }

        @Override
        public HttpServerResponse headersEndHandler(Handler<Void> handler) {
            response.headersEndHandler(handler);
            return this;
        }

        @Override
        public HttpServerResponse bodyEndHandler(Handler<Void> handler) {
            response.bodyEndHandler(handler);
            return this;
        }

        @Override
        public long bytesWritten() {
            return response.bytesWritten();
        }

        @Override
        public int streamId() {
            return response.streamId();
        }

        @Override
        public HttpServerResponse push(HttpMethod method, String host, String path, Handler<AsyncResult<HttpServerResponse>> handler) {
            httpServerRequest.wroteData = true;
            response.push(method, host, path, handler);
            return this;
        }

        @Override
        public HttpServerResponse push(HttpMethod method, String path, MultiMap headers, Handler<AsyncResult<HttpServerResponse>> handler) {
            httpServerRequest.wroteData = true;
            response.push(method, path, headers, handler);
            return this;
        }

        @Override
        public HttpServerResponse push(HttpMethod method, String path, Handler<AsyncResult<HttpServerResponse>> handler) {
            httpServerRequest.wroteData = true;
            response.push(method, path, handler);
            return this;
        }

        @Override
        public HttpServerResponse push(HttpMethod method, String host, String path, MultiMap headers, Handler<AsyncResult<HttpServerResponse>> handler) {
            httpServerRequest.wroteData = true;
            response.push(method, host, path, headers, handler);
            return this;
        }

        @Override
        public void reset(long code) {
            response.reset(code);
        }

        @Override
        public HttpServerResponse writeCustomFrame(int type, int flags, Buffer payload) {
            httpServerRequest.wroteData = true;
            response.writeCustomFrame(type, flags, payload);
            return this;
        }
    }
}
