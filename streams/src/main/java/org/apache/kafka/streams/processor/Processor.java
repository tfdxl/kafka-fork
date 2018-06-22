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
package org.apache.kafka.streams.processor;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * A processor of key-value pair records.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
@InterfaceStability.Evolving
public interface Processor<K, V> {

    /**
     * Initialize this processor with the given context. The framework ensures this is called once per processor when the topology
     * that contains it is initialized.
     * <p>
     * The provided {@link ProcessorContext context} can be used to access topology and record meta data, to
     * {@link ProcessorContext#schedule(long, PunctuationType, Punctuator) schedule} a method to be
     * {@link Punctuator#punctuate(long) called periodically} and to access attached {@link StateStore}s.
     *
     * @param context the context; may not be null
     */
    void init(ProcessorContext context);

    /**
     * Process the record with the given key and value.
     *
     * @param key   the key for the record
     * @param value the value for the record
     */
    void process(K key, V value);

    /**
     * Perform any periodic operations, if this processor {@link ProcessorContext#schedule(long) schedule itself} with the context
     * during {@link #init(ProcessorContext) initialization}.
     *
     * @param timestamp the stream time when this method is being called
     * @deprecated Please use {@link Punctuator} functional interface instead.
     */
    @Deprecated
    void punctuate(long timestamp);

    /**
     * Close this processor and clean up any resources. Be aware that {@link #close()} is called after an internal cleanup.
     * Thus, it is not possible to write anything to Kafka as underlying clients are already closed.
     * <p>
     * Note: Do not close any streams managed resources, like {@link StateStore}s here, as they are managed by the library.
     */
    void close();

    Processor<String, String> DEFAUL_PROCESSOR = new Processor<String, String>() {

        /**
         * 初始化器
         * @param context the context; may not be null
         */
        @Override
        public void init(ProcessorContext context) {

        }

        @Override
        public void process(String key, String value) {

        }

        @Override
        public void punctuate(long timestamp) {

        }

        @Override
        public void close() {

        }
    };

}
