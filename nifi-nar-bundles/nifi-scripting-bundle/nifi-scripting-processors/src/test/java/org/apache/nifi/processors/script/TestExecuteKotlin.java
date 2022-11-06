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
package org.apache.nifi.processors.script;

import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("kotlin")
public class TestExecuteKotlin extends BaseScriptTest {

    @BeforeEach
    public void setup() throws Exception {
        super.setupExecuteScript();
    }

    /**
     * Tests a script that has provides the body of an onTrigger() function.
     *
     */
    @Test
    @EnabledIfSystemProperty(named = "enable.kotlin.support", matches = "true")
    public void testReadFlowFileContentAndStoreInFlowFileAttribute() {
        final TestRunner runner = TestRunners.newTestRunner(new ExecuteScript());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "kotlin");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "src/test/resources/kotlin/test_onTrigger.kts");
        runner.setProperty(ScriptingComponentUtils.MODULES, "src/test/resources/kotlin");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("from-content", "test content");
    }

    @Test
    @EnabledIfSystemProperty(named = "enable.kotlin.support", matches = "true")
    public void testBenchmark() {
        final TestRunner runner = TestRunners.newTestRunner(new ExecuteScript());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "kotlin");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "src/test/resources/kotlin/test_onTrigger.kts");
        runner.setProperty(ScriptingComponentUtils.MODULES, "src/test/resources/kotlin");

        runner.assertValid();
        final int LIMIT = 10000;
        for (int x = 0; x < LIMIT; x++) {
            runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        }
        runner.run(LIMIT);

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, LIMIT);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        assertEquals(LIMIT, result.size());
        result.get(0).assertAttributeEquals("from-content", "test content");
    }
}