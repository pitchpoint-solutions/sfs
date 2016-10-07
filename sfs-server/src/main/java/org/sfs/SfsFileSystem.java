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

import rx.Observable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SfsFileSystem {

    private Path workingDirectory;
    private Path tmpDirectory;
    private Path backupDirectory;

    public SfsFileSystem() {

    }

    public Observable<Void> open(VertxContext<Server> vertxContext, Path workingDirectory) {
        this.workingDirectory = workingDirectory;
        this.tmpDirectory = Paths.get(workingDirectory.toString(), "tmp");
        this.backupDirectory = Paths.get(workingDirectory.toString(), "backup");
        return vertxContext.executeBlocking(() -> {
            try {
                Files.createDirectories(workingDirectory);
                Files.createDirectories(tmpDirectory);
                Files.createDirectories(backupDirectory);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    public Path backupDirectory() {
        return backupDirectory;
    }

    public Path workingDirectory() {
        return workingDirectory;
    }

    public Path tmpDirectory() {
        return tmpDirectory;
    }
}
