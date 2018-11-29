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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.bouncycastle.crypto.Digest;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@CapabilityDescription("Put a description here, it will be shown in NiFi when listing processors")
@Tags({"something", "something_else", "database", "sample", "example"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@ReadsAttributes({
    @ReadsAttribute(attribute = "sample.name", description = "Person name"),
    @ReadsAttribute(attribute = "sample.email", description = "Person's email")
})
@WritesAttributes({
    @WritesAttribute(attribute = "sample.name.hash", description = "Hash of sample.name"),
    @WritesAttribute(attribute = "sample.email.hash", description = "Hash of sample.email")
})
public class SampleProcessor extends AbstractProcessor {
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successful operations send flowfiles here.").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed operations send flowfiles here.").build();

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_SUCCESS, REL_FAILURE
    )));

    public static final PropertyDescriptor SAMPLE_PROPERTY = new PropertyDescriptor.Builder()
        .name("sample-processor-sample-property")
        .displayName("Sample Property")
        .description("A sample property that supports expression language.")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("${sample.name} => ${sample.email}")
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .required(true)
        .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        SAMPLE_PROPERTY
    ));

    public static final String ATTR_SAMPLE_NAME = "sample.name";
    public static final String ATTR_SAMPLE_EMAIL = "sample.email";
    public static final String ATTR_SAMPLE_NAME_HASH = "sample.name.hash";
    public static final String ATTR_SAMPLE_EMAIL_HASH = "sample.email.hash";

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String name) {
        return new PropertyDescriptor.Builder()
            .name(name)
            .displayName(name)
            .description(String.format("Dynamic property: %s", name))
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        try {
            Map<String, String> attrs = input.getAttributes();
            validateAttributes(attrs);
            Map<String, String> hashes = new HashMap<>();
            hashes.put(ATTR_SAMPLE_EMAIL_HASH, DigestUtils.sha256Hex(attrs.get(ATTR_SAMPLE_EMAIL).getBytes()));
            hashes.put(ATTR_SAMPLE_NAME_HASH, DigestUtils.sha256Hex(attrs.get(ATTR_SAMPLE_NAME).getBytes()));

            input = session.putAllAttributes(input, hashes);

            String property = context.getProperty(SAMPLE_PROPERTY).evaluateAttributeExpressions(input).getValue();
            getLogger().info(String.format("Evaluated property as %s", property));

            session.transfer(input, REL_SUCCESS);
            session.getProvenanceReporter().modifyAttributes(input, String.format("Added %s and %s", ATTR_SAMPLE_EMAIL_HASH, ATTR_SAMPLE_NAME_HASH));
        } catch (Exception ex) {
            getLogger().error("Error reading attributes", ex);
            session.transfer(input, REL_FAILURE);
        }
    }

    private void validateAttributes(Map<String, String> attributes) {
        boolean hasName = attributes.containsKey(ATTR_SAMPLE_NAME);
        boolean hasEmail = attributes.containsKey(ATTR_SAMPLE_EMAIL);
        if (!hasName || !hasEmail) {
            throw new ProcessException(String.format("Has name: %s; Has email: %s", hasName, hasEmail));
        }
    }
}
