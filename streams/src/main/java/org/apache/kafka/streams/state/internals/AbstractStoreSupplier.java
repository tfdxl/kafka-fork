/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.StateStore;

import java.util.Map;

@Deprecated
abstract class AbstractStoreSupplier<K, V, T extends StateStore> implements org.apache.kafka.streams.processor.StateStoreSupplier<T> {
    protected final String name;
    protected final Serde<K> keySerde;
    protected final Serde<V> valueSerde;
    protected final Time time;
    protected final boolean logged;
    protected final Map<String, String> logConfig;

    AbstractStoreSupplier(final String name,
                          final Serde<K> keySerde,
                          final Serde<V> valueSerde,
                          final Time time,
                          final boolean logged,
                          final Map<String, String> logConfig) {
        this.time = time;
        this.name = name;
        this.valueSerde = valueSerde;
        this.keySerde = keySerde;
        this.logged = logged;
        this.logConfig = logConfig;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Map<String, String> logConfig() {
        return logConfig;
    }

    @Override
    public boolean loggingEnabled() {
        return logged;
    }
}
