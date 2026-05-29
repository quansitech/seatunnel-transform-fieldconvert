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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class FieldConvertTransform extends MultipleFieldOutputTransform {

    public static final String PLUGIN_NAME = "FieldConvert";

    private final List<FieldConvertRule> rules;
    private final int[] fieldIndices;

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    public FieldConvertTransform(List<FieldConvertRule> rules, CatalogTable catalogTable) {
        super(catalogTable);
        SeaTunnelRowType rowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        List<String> columnNames = new ArrayList<>();
        for (Column col : catalogTable.getTableSchema().getColumns()) {
            columnNames.add(col.getName());
        }
        // Filter rules: field must exist in both rowType AND catalog columns, with compatible type
        List<FieldConvertRule> applicableRules = new ArrayList<>();
        List<Integer> indices = new ArrayList<>();
        for (FieldConvertRule rule : rules) {
            int index = rowType.indexOf(rule.getField());
            if (index >= 0
                    && columnNames.contains(rule.getField())
                    && isTypeCompatible(rule, rowType.getFieldType(index))) {
                applicableRules.add(rule);
                indices.add(index);
            }
        }
        this.rules = applicableRules;
        this.fieldIndices = new int[applicableRules.size()];
        for (int i = 0; i < indices.size(); i++) {
            fieldIndices[i] = indices.get(i);
        }
    }

    private boolean isTypeCompatible(FieldConvertRule rule, SeaTunnelDataType<?> fieldType) {
        switch (rule.getMethod().toLowerCase()) {
            case "unix_timestamp_to_datetime":
                return isNumericType(fieldType);
            case "cast":
                return true;
            default:
                return true;
        }
    }

    private boolean isNumericType(SeaTunnelDataType<?> type) {
        return type == BasicType.INT_TYPE
                || type == BasicType.LONG_TYPE
                || type == BasicType.SHORT_TYPE
                || type == BasicType.BYTE_TYPE
                || type == BasicType.DOUBLE_TYPE
                || type == BasicType.FLOAT_TYPE
                || type instanceof DecimalType;
    }

    @Override
    protected Column[] getOutputColumns() {
        Column[] columns = new Column[rules.size()];
        for (int i = 0; i < rules.size(); i++) {
            FieldConvertRule rule = rules.get(i);
            SeaTunnelDataType<?> targetType = resolveTargetType(rule);

            Column originalColumn =
                    inputCatalogTable.getTableSchema().getColumns().stream()
                            .filter(c -> c.getName().equals(rule.getField()))
                            .findFirst()
                            .orElse(null);

            columns[i] =
                    PhysicalColumn.of(
                            rule.getField(),
                            targetType,
                            originalColumn != null && originalColumn.getColumnLength() != null
                                    ? originalColumn.getColumnLength() : 0L,
                            originalColumn == null || originalColumn.isNullable(),
                            originalColumn != null ? originalColumn.getDefaultValue() : null,
                            originalColumn != null ? originalColumn.getComment() : null);
        }
        return columns;
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        Object[] values = new Object[rules.size()];
        for (int i = 0; i < rules.size(); i++) {
            Object value = inputRow.getField(fieldIndices[i]);
            values[i] = convertValue(value, rules.get(i));
        }
        return values;
    }

    private SeaTunnelDataType<?> resolveTargetType(FieldConvertRule rule) {
        switch (rule.getMethod().toLowerCase()) {
            case "unix_timestamp_to_datetime":
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case "cast":
                return parseBasicType(rule.getTargetType());
            default:
                throw new IllegalArgumentException(
                        "Unsupported convert method: " + rule.getMethod());
        }
    }

    private SeaTunnelDataType<?> parseBasicType(String targetType) {
        if (targetType == null || targetType.isEmpty()) {
            throw new IllegalArgumentException("target_type is required for cast method");
        }
        switch (targetType.toLowerCase()) {
            case "int":
            case "integer":
                return BasicType.INT_TYPE;
            case "long":
                return BasicType.LONG_TYPE;
            case "short":
                return BasicType.SHORT_TYPE;
            case "byte":
                return BasicType.BYTE_TYPE;
            case "double":
                return BasicType.DOUBLE_TYPE;
            case "float":
                return BasicType.FLOAT_TYPE;
            case "string":
                return BasicType.STRING_TYPE;
            case "boolean":
                return BasicType.BOOLEAN_TYPE;
            default:
                throw new IllegalArgumentException("Unsupported target_type: " + targetType);
        }
    }

    private Object convertValue(Object value, FieldConvertRule rule) {
        if (value == null) {
            return null;
        }
        switch (rule.getMethod().toLowerCase()) {
            case "unix_timestamp_to_datetime":
                return convertUnixTimestamp(value, rule.getTimezone());
            case "cast":
                return castValue(value, rule.getTargetType());
            default:
                throw new IllegalArgumentException(
                        "Unsupported convert method: " + rule.getMethod());
        }
    }

    private LocalDateTime convertUnixTimestamp(Object value, String timezone) {
        long timestamp;
        if (value instanceof Number) {
            timestamp = ((Number) value).longValue();
        } else {
            throw new IllegalArgumentException(
                    "unix_timestamp_to_datetime expects Number, got: " + value.getClass().getName());
        }
        ZoneId zoneId =
                timezone != null && !timezone.isEmpty()
                        ? ZoneId.of(timezone)
                        : ZoneId.systemDefault();
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), zoneId);
    }

    private Object castValue(Object value, String targetType) {
        if (targetType == null) {
            return value;
        }
        if (value instanceof Boolean) {
            boolean b = (Boolean) value;
            switch (targetType.toLowerCase()) {
                case "int":
                case "integer":
                    return b ? 1 : 0;
                case "long":
                    return b ? 1L : 0L;
                case "short":
                    return b ? (short) 1 : (short) 0;
                case "byte":
                    return b ? (byte) 1 : (byte) 0;
                case "double":
                    return b ? 1.0 : 0.0;
                case "float":
                    return b ? 1.0f : 0.0f;
                case "string":
                    return Boolean.toString(b);
                default:
                    return value;
            }
        }
        if (value instanceof Number) {
            Number num = (Number) value;
            switch (targetType.toLowerCase()) {
                case "int":
                case "integer":
                    return num.intValue();
                case "long":
                    return num.longValue();
                case "short":
                    return num.shortValue();
                case "byte":
                    return num.byteValue();
                case "double":
                    return num.doubleValue();
                case "float":
                    return num.floatValue();
                default:
                    break;
            }
        }
        switch (targetType.toLowerCase()) {
            case "string":
                return value.toString();
            case "boolean":
                if (value instanceof Number) {
                    return ((Number) value).intValue() != 0;
                }
                return Boolean.parseBoolean(value.toString());
            default:
                return value;
        }
    }
}
