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
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.sample.SampleControllerService;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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

    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Failed flowfiles go here.")
        .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Successful flowfiles go here.")
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        LOOKUP_SERVICE, SERVICE
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_FAILURE, REL_SUCCESS
    )));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

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

        for (int index = 0; index < 20; index++) {
            sampleControllerService.doSomething(); //Just sends log statements.
        }

        try {
            Optional result = lookupService.lookup(input.getAttributes());
            if (result.isPresent()) {
                input = session.write(input, out -> out.write(result.get().toString().getBytes()));
                session.transfer(input, REL_SUCCESS);
                session.getProvenanceReporter().modifyContent(input);
            } else {
                session.transfer(input, REL_FAILURE);
            }
        } catch (Exception ex) {
            getLogger().error("Error processing something.", ex);
            session.transfer(input, REL_FAILURE);
        }
    }
}
