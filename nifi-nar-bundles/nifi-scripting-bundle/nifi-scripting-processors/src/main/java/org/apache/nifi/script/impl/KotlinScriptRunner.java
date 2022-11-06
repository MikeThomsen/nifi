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
package org.apache.nifi.script.impl;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

public class KotlinScriptRunner extends BaseScriptRunner {
    private final CompiledScript compiledScript;

    private static final String PRELOADS =
            "import org.apache.nifi.components.*\n"
                    + "import org.apache.nifi.flowfile.FlowFile\n"
                    + "import org.apache.nifi.processor.*\n"
                    + "import org.apache.nifi.processor.exception.*\n"
                    + "import org.apache.nifi.processor.io.*\n"
                    + "import org.apache.nifi.processor.util.*\n"
                    + "import org.apache.nifi.processors.script.*\n"
                    + "import org.apache.nifi.logging.ComponentLog\n"
                    + "import org.apache.nifi.script.*\n"
                    + "import org.apache.nifi.record.sink.*\n"
                    + "import org.apache.nifi.lookup.*\n\n\n"
                    + "val session = bindings[\"session\"] as org.apache.nifi.processor.ProcessSession\n"
                    + "val REL_SUCCESS = bindings[\"REL_SUCCESS\"] as org.apache.nifi.processor.Relationship\n"
                    + "val REL_FAILURE = bindings[\"REL_FAILURE\"] as org.apache.nifi.processor.Relationship";


    public KotlinScriptRunner(ScriptEngine engine, String scriptBody, String[] modulePaths) throws ScriptException {
        super(engine, scriptBody, PRELOADS, modulePaths);
        compiledScript = ((Compilable) engine).compile(this.scriptBody);
    }

    @Override
    public String getScriptEngineName() {
        return "kts";
    }

    @Override
    public void run(Bindings bindings) throws ScriptException {
        if (compiledScript == null) {
            throw new ScriptException("Kotlin script has not been successfully compiled");
        }
        compiledScript.eval(bindings);
    }
}
