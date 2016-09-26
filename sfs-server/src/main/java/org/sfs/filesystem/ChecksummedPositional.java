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

package org.sfs.filesystem;

import static com.google.common.io.BaseEncoding.base64;

public abstract class ChecksummedPositional<A> extends Positional<A> {

    private final byte[] actualChecksum;

    public ChecksummedPositional(long position, A value, byte[] actualChecksum) {
        super(position, value);
        this.actualChecksum = actualChecksum;
    }

    public byte[] getActualChecksum() {
        return actualChecksum;
    }

    public abstract boolean isChecksumValid();

    @Override
    public String toString() {
        return "VersionedPositional{" +
                ", actualChecksum=" + base64().encode(actualChecksum) +
                "} " + super.toString();
    }
}
