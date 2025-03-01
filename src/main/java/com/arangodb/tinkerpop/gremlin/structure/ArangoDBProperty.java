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

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class ArangoDBProperty<V> implements Property<V> {

    private final String key;
    private final V value;
    private final ArangoDBElement<?, ?> element;

    public ArangoDBProperty(final ArangoDBElement<?, ?> element, final String key, final V value) {
        this.element = element;
        this.key = key;
        this.value = value;
    }

    @Override
    public ArangoDBElement<?, ?> element() {
        return element;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public V value() {
        return value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public void remove() {
        element.removeProperty(key);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

}

