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

package org.apache.nifi.sample;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class SampleLookupService extends AbstractControllerService implements LookupService<String> {
    public static final String ATTR_NAME = "name";
    public static final String ATTR_EMAIL = "email";

    /*
     * Key/value pairs that are added to the LookupRecord/Attribute processors are sent here.
     */
    @Override
    public Optional<String> lookup(Map<String, Object> map) throws LookupFailureException {
        return Optional.ofNullable(hash(map));
    }

    @Override
    public Optional<String> lookup(Map<String, Object> map, Map<String, String> flowFileAttributes) throws LookupFailureException {
        return lookup(map);
    }

    private String hash(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder()
            .append(map.get(ATTR_NAME))
            .append("-")
            .append(map.get(ATTR_EMAIL));
        return DigestUtils.sha256Hex(sb.toString().getBytes());
    }

    /*
     * Used to enforce the return type of the lookup operation.
     */
    @Override
    public Class<?> getValueType() {
        return String.class;
    }

    /*
     * Required by the LookupRecord/Attribute processor to know what keys are mandatory. Empty set means anything goes.
     */
    @Override
    public Set<String> getRequiredKeys() {
        return new HashSet<>(Arrays.asList("email", "name"));
    }
}
