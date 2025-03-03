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

import java.util.*;

public class EdgeData extends PersistentData<AdbValue> {

    @InternalFrom
    private String from;

    @InternalTo
    private String to;

    private final Map<String, AdbValue> properties = new HashMap<>();

    public EdgeData() {
    }

    public EdgeData(
            String label,
            String key,
            String from,
            String to
    ) {
        super(label, key);
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");

        this.from = from;
        this.to = to;
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
    public Map<String, AdbValue> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "EdgeData{" +
                "from='" + from + '\'' +
                ", to='" + to + '\'' +
                ", properties=" + properties +
                ", super=" + super.toString() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof EdgeData)) return false;
        if (!super.equals(o)) return false;
        EdgeData edgeData = (EdgeData) o;
        return Objects.equals(from, edgeData.from) && Objects.equals(to, edgeData.to) && Objects.equals(properties, edgeData.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), from, to, properties);
    }
}
