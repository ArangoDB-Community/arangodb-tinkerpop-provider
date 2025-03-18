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

package com.arangodb.tinkerpop.gremlin.persistence;

import com.arangodb.serde.*;
import com.arangodb.shaded.fasterxml.jackson.annotation.JsonProperty;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBId;

import java.util.*;

public class EdgeData extends SimplePropertyData implements PersistentData {

    @InternalId
    private ArangoDBId id;

    @JsonProperty
    private String label;

    @InternalKey
    private String key;

    @InternalFrom
    private ArangoDBId from;

    @InternalTo
    private ArangoDBId to;

    public static EdgeData of(
            ArangoDBId id,
            ArangoDBId from,
            ArangoDBId to
    ) {
        EdgeData data = new EdgeData();
        data.id = id;
        data.label = id.getLabel();
        data.key = id.getKey();
        data.from = from;
        data.to = to;
        return data;
    }

    public EdgeData() {
    }

    @Override
    public ArangoDBId getId() {
        return id;
    }

    @Override
    public void setId(ArangoDBId id) {
        this.id = id;
    }

    @Override
    public void setKey(String key) {
        this.key = key;
    }

    public ArangoDBId getFrom() {
        return from;
    }

    public void setFrom(ArangoDBId from) {
        this.from = from;
    }

    public ArangoDBId getTo() {
        return to;
    }

    public void setTo(ArangoDBId to) {
        this.to = to;
    }

    @Override
    public String toString() {
        return "EdgeData{" +
                "from=" + from +
                ", id=" + id +
                ", label='" + label + '\'' +
                ", key='" + key + '\'' +
                ", to=" + to +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof EdgeData)) return false;
        if (!super.equals(o)) return false;
        EdgeData edgeData = (EdgeData) o;
        return Objects.equals(id, edgeData.id) && Objects.equals(label, edgeData.label) && Objects.equals(key, edgeData.key) && Objects.equals(from, edgeData.from) && Objects.equals(to, edgeData.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), id, label, key, from, to);
    }
}
