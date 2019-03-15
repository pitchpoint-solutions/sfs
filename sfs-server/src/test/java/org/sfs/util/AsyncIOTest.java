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

package org.sfs.util;

import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.io.AsyncIO;
import org.sfs.io.BufferWriteEndableWriteStream;
import org.sfs.io.EndableReadStream;
import rx.functions.Func1;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class AsyncIOTest extends BaseTestVerticle {

    @Test
    public void copyZeroLength(TestContext context) throws IOException {
        runOnServerContext(context, () -> {
            Path tmpFile = Files.createTempFile(tmpDir(), "", "");

            AsyncFile asyncFile = vertx().fileSystem().openBlocking(tmpFile.toString(), new OpenOptions());
            final BufferWriteEndableWriteStream bufferWriteStream = new BufferWriteEndableWriteStream();

            return AsyncIO.pump(EndableReadStream.from(asyncFile), bufferWriteStream)
                    .map(new Func1<Void, Void>() {
                        @Override
                        public Void call(Void aVoid) {
                            VertxAssert.assertEquals(context, 0, bufferWriteStream.toBuffer().length());
                            return null;
                        }
                    });
        });
    }
}