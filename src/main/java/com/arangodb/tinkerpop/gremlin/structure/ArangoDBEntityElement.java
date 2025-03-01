/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.tinkerpop.gremlin.persistence.AbstractElementData;

import java.util.*;

public abstract class ArangoDBEntityElement<P, D extends AbstractElementData<P>> extends ArangoDBElement<P, D> {

    public ArangoDBEntityElement(ArangoDBGraph graph, D data) {
        super(graph, data);
    }

    @Override
    public String id() {
        return Optional.ofNullable(key())
                .map(it -> collection() + '/' + it)
                .orElse(label());
    }

    public String key() {
        return data.getKey();
    }

    public void key(String key) {
        data.setKey(key);
    }

    @Override
    public String label() {
        return data.getLabel();
    }

    public String collection() {
        return graph.getPrefixedCollectioName(label());
    }
}
