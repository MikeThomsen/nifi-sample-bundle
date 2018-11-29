/*
 * Sample notice if you want to submit work to the ASF:
 *
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

package org.apache.nifi.processor.sample

import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Before
import org.junit.Test

class SampleProcessorTest {
    TestRunner runner
    @Before
    void setup() {
        runner = TestRunners.newTestRunner(SampleProcessor.class)

    }

    @Test
    void testValid() {
        runner.assertValid()
        runner.enqueue("Test", [
            (SampleProcessor.ATTR_SAMPLE_EMAIL): "test@test.com",
            (SampleProcessor.ATTR_SAMPLE_NAME): "Test User"
        ])
        runner.run()
        runner.assertTransferCount(SampleProcessor.REL_FAILURE, 0)
        runner.assertTransferCount(SampleProcessor.REL_SUCCESS, 1)
    }

    @Test
    void testMissingProperties() {
        runner.assertValid()
        runner.enqueue("Test")
        runner.run()
        runner.assertTransferCount(SampleProcessor.REL_SUCCESS, 0)
        runner.assertTransferCount(SampleProcessor.REL_FAILURE, 1)
    }
}