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

public abstract class AbstractNode<T extends AbstractNode> implements XNode<T> {

    private final String groupId;

    public AbstractNode() {
        this.groupId = "default";
    }

    @Override
    public String getGroupId() {
        return groupId;
    }

    @Override
    public boolean isSameGroup(XNode xNode) {
        if (xNode == null) {
            return false;
        }
        String oThisGroupId = getGroupId();
        String oOtherGroupId = xNode.getGroupId();
        return oOtherGroupId.equals(oThisGroupId);
    }

}
