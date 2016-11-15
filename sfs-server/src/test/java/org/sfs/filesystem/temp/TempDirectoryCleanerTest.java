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

package org.sfs.filesystem.temp;

import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.sfs.integration.java.BaseTestVerticle;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.exists;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.sfs.util.VertxAssert.assertFalse;
import static org.sfs.util.VertxAssert.assertTrue;

public class TempDirectoryCleanerTest extends BaseTestVerticle {


    @Test
    public void testDelete(TestContext context) throws IOException {

        Path tempDir = vertxContext.verticle().sfsFileSystem().tmpDirectory();

        TempDirectoryCleaner tempDirectoryCleaner = vertxContext.verticle().tempFileFactory();

        List<Path> tempFiles = new ArrayList<>();

        tempFiles.add(createTempFile(tempDir, "1", ""));
        tempFiles.add(createTempFile(tempDir, "2", ""));
        tempFiles.add(createTempFile(tempDir, "3", ""));
        tempFiles.add(createTempFile(tempDir, "4", ""));

        tempDirectoryCleaner.start(vertxContext, 0);


        tempDirectoryCleaner.deleteExpired();

        for (Path tempFile : tempFiles) {
            assertFalse(context, exists(tempFile));
        }

        tempFiles.clear();

        tempFiles.add(createTempFile(tempDir, "1", ""));
        tempFiles.add(createTempFile(tempDir, "2", ""));
        tempFiles.add(createTempFile(tempDir, "3", ""));
        tempFiles.add(createTempFile(tempDir, "4", ""));

        tempDirectoryCleaner.stop();
        tempDirectoryCleaner.start(vertxContext, DAYS.toMillis(1));

        tempDirectoryCleaner.deleteExpired();

        for (Path tempFile : tempFiles) {
            assertTrue(context, exists(tempFile));
        }

    }
}