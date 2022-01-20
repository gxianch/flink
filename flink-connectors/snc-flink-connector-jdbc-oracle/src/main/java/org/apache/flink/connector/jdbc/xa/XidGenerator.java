/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;

import javax.transaction.xa.Xid;

import java.io.Serializable;
import java.security.SecureRandom;

/** {@link Xid} generator. */
@Internal
public interface XidGenerator extends Serializable, AutoCloseable {

    /**
     * Generate new {@link Xid}. Requirements for generated Xids:
     *
     * <ul>
     *   <li>Global Transaction Id MUST be unique across Flink job, and probably across Xids
     *       generated by other jobs and applications - depends on the usage of this class
     *   <li>SHOULD be immutable
     *   <li>SHOULD override {@link Object#hashCode hashCode} and {@link Object#equals equals}
     * </ul>
     *
     * @param runtimeContext can be used for example to derive global transaction id
     * @param checkpointId can be used for example to derive global transaction id
     */
    Xid generateXid(RuntimeContext runtimeContext, long checkpointId);

    default void open() {}

    /** @return true if the provided transaction belongs to this subtask */
    boolean belongsToSubtask(Xid xid, RuntimeContext ctx);

    @Override
    default void close() {}

    /**
     * Creates a {@link XidGenerator} that generates {@link Xid xids} from:
     *
     * <ol>
     *   <li>job id
     *   <li>subtask index
     *   <li>checkpoint id
     *   <li>four random bytes generated using {@link SecureRandom})
     * </ol>
     *
     * <p>Each created {@link XidGenerator} instance MUST be used for only one Sink instance
     * (otherwise Xids could collide).
     */
    static XidGenerator semanticXidGenerator() {
        return new SemanticXidGenerator();
    }
}
