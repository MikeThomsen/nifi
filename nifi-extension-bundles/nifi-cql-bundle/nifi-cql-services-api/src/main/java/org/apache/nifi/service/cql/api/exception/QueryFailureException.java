/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.service.cql.api.exception;

/**
 * Signals that a {@code CQLExecutionService} call failed at the driver/server level - for example, a query
 * execution failure or a failure to reach any cluster node - as opposed to a configuration or data problem
 * (which implementations may instead surface as an unchecked exception such as {@link IllegalArgumentException}).
 * Callers (such as {@code PutCQLRecord}/{@code ExecuteCQLQueryRecord}) generally treat this distinction as
 * "may succeed if retried" versus "will not succeed without a configuration or data change".
 */
public class QueryFailureException extends RuntimeException {
}
