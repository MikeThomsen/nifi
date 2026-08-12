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

package org.apache.nifi.service.scylladb;

import org.apache.nifi.service.cql.it.DockerUtils;
import org.testcontainers.scylladb.ScyllaDBContainer;

import java.util.Map;

/**
 * The resource ceiling every ScyllaDB container in this package runs under. Left to itself ScyllaDB sizes
 * against the whole machine, so an integration run could take as much memory and as many cores as the host
 * had; none of these suites need more than a single shard and a few hundred megabytes.
 *
 * <p>Applied in one place because the settings below are interdependent, and getting any one of them wrong
 * fails startup in a way whose error message points somewhere else entirely:
 *
 * <ul>
 *   <li><b>{@code mode=1777} on the tmpfs.</b> The mount masks the image's {@code scylla}-owned
 *       {@code /var/lib/scylla}, and Docker mounts a tmpfs root-owned {@code 0755} by default. The image
 *       runs as a non-root user, so without this the entrypoint dies on {@code Permission denied} creating
 *       the data directory, before ScyllaDB starts at all.</li>
 *   <li><b>{@code --reserve-memory}.</b> Seastar holds back roughly 1.5 GB of whatever limit it sees for
 *       the OS, so inside the {@value #MEMORY_GB} GB cap below it decides only ~500 MB is left and refuses
 *       to start with the {@value #SCYLLA_MEMORY} asked for - "insufficient physical memory". Lowering the
 *       reserve is what makes a small container workable.</li>
 *   <li><b>The two commitlog sizes.</b> Out of the box ScyllaDB preallocates roughly 450 MB of commitlog -
 *       384 MB of it the schema commitlog, six segments at its 64 MB default - which all but fills the tmpfs
 *       below before a single row is written. Suites whose {@code scylla.yaml} is replaced wholesale
 *       ({@code ScyllaSslIT}, {@code ScyllaAuthenticationIT}) tip it over entirely and the node shuts its
 *       CQL port down with "No space left on device" moments after reporting startup, which surfaces in the
 *       test as a closed connection rather than as anything about disk. These suites write kilobytes, so
 *       capping both brings steady-state usage to about 20% of the tmpfs.</li>
 *   <li><b>{@code --developer-mode} and {@code --overprovisioned}.</b> Both come from
 *       {@link ScyllaDBContainer}'s own default command, which {@code withCommand} replaces wholesale, so
 *       they have to be repeated here. Without {@code --overprovisioned} ScyllaDB assumes it has dedicated
 *       cores it does not have on a test host.</li>
 * </ul>
 */
final class ScyllaContainerLimits {

    private static final long CPUS = 2;

    private static final long MEMORY_GB = 2;

    private static final String SCYLLA_MEMORY = "750M";

    private static final String SCYLLA_RESERVE_MEMORY = "256M";

    private static final String COMMITLOG_TOTAL_SPACE_MB = "64";

    private static final String SCHEMA_COMMITLOG_SEGMENT_SIZE_MB = "16";

    private static final String DATA_DIRECTORY = "/var/lib/scylla";

    private static final String DATA_TMPFS_OPTIONS = "rw,size=512m,mode=1777";

    private ScyllaContainerLimits() {
    }

    /**
     * Caps the container's host resources and sizes ScyllaDB to fit inside them, backing its data directory
     * with a tmpfs so these suites' writes never touch the host disk.
     *
     * @param container the container to constrain, before it is started
     * @return the same container, for chaining
     */
    static ScyllaDBContainer apply(final ScyllaDBContainer container) {
        return container
                .withTmpFs(Map.of(DATA_DIRECTORY, DATA_TMPFS_OPTIONS))
                .withCommand("--smp", "1", "--memory", SCYLLA_MEMORY, "--reserve-memory", SCYLLA_RESERVE_MEMORY,
                        "--commitlog-total-space-in-mb", COMMITLOG_TOTAL_SPACE_MB,
                        "--schema-commitlog-segment-size-in-mb", SCHEMA_COMMITLOG_SEGMENT_SIZE_MB,
                        "--developer-mode", "1", "--overprovisioned", "1")
                .withCreateContainerCmdModifier(DockerUtils.createMemoryLimits(CPUS, MEMORY_GB));
    }
}
