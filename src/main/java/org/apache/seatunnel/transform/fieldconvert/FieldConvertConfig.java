/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.transform.fieldconvert;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FieldConvertConfig implements Serializable {

    public static final Option<List<Map<String, Object>>> RULES =
            Options.key("rules")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription(
                            "Field conversion rules. Each rule has: "
                                    + "table_pattern (glob pattern like 'db.*'), "
                                    + "field (field name), "
                                    + "method (unix_timestamp_to_datetime | cast), "
                                    + "timezone (for unix_timestamp_to_datetime), "
                                    + "target_type (for cast: int|long|short|byte|double|float|string|boolean)");

    private final List<FieldConvertRule> rules;

    private FieldConvertConfig(List<FieldConvertRule> rules) {
        this.rules = rules;
    }

    public static FieldConvertConfig of(List<Map<String, Object>> rawRules) {
        List<FieldConvertRule> rules = new ArrayList<>();
        if (rawRules != null) {
            for (Map<String, Object> raw : rawRules) {
                rules.add(
                        new FieldConvertRule(
                                getString(raw, "table_pattern", ".*"),
                                getString(raw, "field", null),
                                getString(raw, "method", null),
                                getString(raw, "timezone", "UTC"),
                                getString(raw, "target_type", null)));
            }
        }
        return new FieldConvertConfig(rules);
    }

    public List<FieldConvertRule> getRules() {
        return rules;
    }

    /** Return all rules that match the given table path (format: database.table). */
    public List<FieldConvertRule> getMatchingRules(String tablePath) {
        List<FieldConvertRule> matched = new ArrayList<>();
        for (FieldConvertRule rule : rules) {
            String pattern = rule.getTablePattern();
            if (pattern == null || pattern.isEmpty() || pattern.equals(".*")) {
                matched.add(rule);
            } else if (pattern.contains("*") || pattern.contains("?")) {
                // Glob pattern: "donation.*" matches "donation.qs_book_info"
                String regex = globToRegex(pattern);
                if (tablePath.matches(regex)) {
                    matched.add(rule);
                }
            } else {
                // Exact match
                if (tablePath.equals(pattern)) {
                    matched.add(rule);
                }
            }
        }
        return matched;
    }

    private static String globToRegex(String glob) {
        StringBuilder sb = new StringBuilder("^");
        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
            switch (c) {
                case '*':
                    sb.append(".*");
                    break;
                case '?':
                    sb.append(".");
                    break;
                case '.':
                    sb.append("\\.");
                    break;
                default:
                    sb.append(c);
            }
        }
        sb.append("$");
        return sb.toString();
    }

    private static String getString(Map<String, Object> map, String key, String defaultValue) {
        Object value = map.get(key);
        return value != null ? value.toString() : defaultValue;
    }
}
