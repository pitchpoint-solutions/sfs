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

package org.sfs.nodes;

import com.google.common.base.Optional;
import com.google.common.net.HostAndPort;
import org.sfs.filesystem.volume.DigestBlob;
import org.sfs.filesystem.volume.HeaderBlob;
import org.sfs.filesystem.volume.ReadStreamBlob;
import org.sfs.util.MessageDigestFactory;
import rx.Observable;

public interface XNode<T extends XNode> {

    HostAndPort getHostAndPort();

    String getGroupId();

    boolean isLocal();

    boolean isSameGroup(XNode xNode);

    Observable<Optional<DigestBlob>> checksum(String volumeId, long position, Optional<Long> oOffset, Optional<Long> oLength, MessageDigestFactory... messageDigestFactories);

    Observable<Optional<HeaderBlob>> acknowledge(String volumeId, long position);

    Observable<Optional<HeaderBlob>> delete(String volumeId, final long position);

    Observable<Optional<ReadStreamBlob>> createReadStream(String volumeId, long position, Optional<Long> offset, Optional<Long> length);

    Observable<Boolean> canPut(String volumeId);

    Observable<NodeWriteStreamBlob> createWriteStream(String volumeId, long length, MessageDigestFactory... messageDigestFactories);
}
