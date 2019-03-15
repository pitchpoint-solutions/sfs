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

import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import static java.lang.String.format;

public class InsufficientReplicaVolumesAvailableException extends RuntimeException {

    public InsufficientReplicaVolumesAvailableException(int expected, long requiredSpace, List<VolumeReplicaGroup.ConnectedVolume> connectedVolumes, NavigableMap<Long, Set<String>> volumesBySpace) {
        super(format("Expected: %s, Available: %d, Required space: %d, Connected Volumes: %s, Available Volumes %s", expected, connectedVolumes.size(), requiredSpace, connectedVolumes, volumesBySpace));
    }
}
