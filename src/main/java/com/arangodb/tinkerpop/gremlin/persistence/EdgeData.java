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
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.*;

public class EdgeData extends SimplePropertyData implements PersistentData {

    @JsonProperty
    private String label;

    @InternalKey
    private String key;

    @InternalFrom
    private String from;

    @InternalTo
    private String to;

    public static EdgeData of(
            String label,
            String key,
            String from,
            String to
    ) {
        ElementHelper.validateLabel(label);
        if (key != null && key.isEmpty()) throw new IllegalArgumentException("empty key");
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");

        EdgeData data = new EdgeData();
        data.label = label;
        data.key = key;
        data.from = from;
        data.to = to;
        return data;
    }

    public EdgeData() {
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public void setKey(String key) {
        this.key = key;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    @Override
    public String toString() {
        return "EdgeData{" +
                "from='" + from + '\'' +
                ", label='" + label + '\'' +
                ", key='" + key + '\'' +
                ", to='" + to + '\'' +
                ", super=" + super.toString() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof EdgeData)) return false;
        EdgeData edgeData = (EdgeData) o;
        return Objects.equals(label, edgeData.label) && Objects.equals(key, edgeData.key) && Objects.equals(from, edgeData.from) && Objects.equals(to, edgeData.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, key, from, to);
    }
}
