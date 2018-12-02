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

package org.apache.nifi.processor.sample;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.sample.SampleControllerService;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class ServiceDemonstrationProcessor extends AbstractProcessor {
    public static final PropertyDescriptor LOOKUP_SERVICE = new PropertyDescriptor.Builder()
        .name("lookup-service")
        .displayName("Lookup Service")
        .description("Configure a lookup service")
        .required(true)
        .identifiesControllerService(LookupService.class)
        .build();

    public static final PropertyDescriptor SERVICE = new PropertyDescriptor.Builder()
        .name("controller-service")
        .displayName("Controller Service")
        .description("Configure a controller service")
        .required(true)
        .identifiesControllerService(SampleControllerService.class)
        .build();

    private volatile LookupService lookupService;
    private volatile SampleControllerService sampleControllerService;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        this.lookupService = context.getProperty(LOOKUP_SERVICE).asControllerService(LookupService.class);
        this.sampleControllerService = context.getProperty(SERVICE).asControllerService(SampleControllerService.class);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }
    }
}
