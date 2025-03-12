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

import com.arangodb.tinkerpop.gremlin.persistence.PersistentData;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Optional;

public interface ArangoDBPersistentElement extends Element {

    @Override
    ArangoDBGraph graph();

    PersistentData data();

    default String key() {
        return data().getKey();
    }

    default void key(String key) {
        data().setKey(key);
    }

    @Override
    default String label() {
        return data().getLabel();
    }

    @SuppressWarnings("resource")
    default String collection() {
        return graph().getPrefixedCollectioName(label());
    }

    @Override
    default String id() {
        return Optional.ofNullable(key())
                .map(it -> collection() + '/' + it)
                // TODO: review
                .orElse(label());
    }
}
