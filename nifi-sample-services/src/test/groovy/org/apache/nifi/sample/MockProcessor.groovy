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

package org.apache.nifi.sample

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.exception.ProcessException

class MockProcessor extends AbstractProcessor {
    static final PropertyDescriptor LOOKUP = new PropertyDescriptor.Builder()
        .name("lookup")
        .identifiesControllerService(SampleLookupService.class)
        .required(false)
        .build()
    static final PropertyDescriptor SAMPLE = new PropertyDescriptor.Builder()
            .name("sample")
            .identifiesControllerService(SampleControllerService.class)
            .required(false)
            .build()

    static final List<PropertyDescriptor> PROPERTIES = [ LOOKUP, SAMPLE ]

    @Override
    List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        PROPERTIES
    }

    @Override
    void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

    }
}
