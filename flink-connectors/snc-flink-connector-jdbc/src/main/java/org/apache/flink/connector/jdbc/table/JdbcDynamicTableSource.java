/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** A {@link DynamicTableSource} for JDBC. */
@Internal
public class JdbcDynamicTableSource
        implements ScanTableSource,
                LookupTableSource,
                SupportsProjectionPushDown,
                SupportsLimitPushDown {

    private final JdbcConnectorOptions options;
    private final JdbcReadOptions readOptions;
    private final JdbcLookupOptions lookupOptions;
    private DataType physicalRowDataType;
    private final String dialectName;
    private long limit = -1;

    public static List<String> getFieldNames(DataType dataType) {
        final LogicalType type = dataType.getLogicalType();
        if (type.getTypeRoot() == LogicalTypeRoot.DISTINCT_TYPE) {
            return getFieldNames(dataType.getChildren().get(0));
        } else if (isCompositeType(type)) {
            return LogicalTypeChecks.getFieldNames(type);
        }
        return Collections.emptyList();
    }

    public static List<DataType> getFieldDataTypes(DataType dataType) {
        final LogicalType type = dataType.getLogicalType();
        if (type.getTypeRoot() == LogicalTypeRoot.DISTINCT_TYPE) {
            return getFieldDataTypes(dataType.getChildren().get(0));
        } else if (isCompositeType(type)) {
            return dataType.getChildren();
        }
        return Collections.emptyList();
    }

    public static boolean isCompositeType(LogicalType logicalType) {
        if (logicalType instanceof DistinctType) {
            return isCompositeType(((DistinctType) logicalType).getSourceType());
        }

        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();
        return typeRoot == LogicalTypeRoot.STRUCTURED_TYPE || typeRoot == LogicalTypeRoot.ROW;
    }

    public int[] getPrimaryKeyIndexes(ResolvedSchema resolvedSchema) {
        final List<String> columns =
                resolvedSchema.getColumns().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList());
        return resolvedSchema
                .getPrimaryKey()
                .map(UniqueConstraint::getColumns)
                .map(pkColumns -> pkColumns.stream().mapToInt(columns::indexOf).toArray())
                .orElseGet(() -> new int[] {});
    }

    public JdbcDynamicTableSource(
            JdbcConnectorOptions options,
            JdbcReadOptions readOptions,
            JdbcLookupOptions lookupOptions,
            DataType physicalRowDataType) {
        this.options = options;
        this.readOptions = readOptions;
        this.lookupOptions = lookupOptions;
        this.physicalRowDataType = physicalRowDataType;
        this.dialectName = options.getDialect().dialectName();
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // JDBC only support non-nested look up keys
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "JDBC only support non-nested look up keys");
            keyNames[i] = getFieldNames(physicalRowDataType).get(innerKeyArr[0]);
        }
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();

        return TableFunctionProvider.of(
                new JdbcRowDataLookupFunction(
                        options,
                        lookupOptions,
                        getFieldNames(physicalRowDataType).toArray(new String[0]),
                        getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]),
                        keyNames,
                        rowType));
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final JdbcRowDataInputFormat.Builder builder =
                JdbcRowDataInputFormat.builder()
                        .setDrivername(options.getDriverName())
                        .setDBUrl(options.getDbURL())
                        .setUsername(options.getUsername().orElse(null))
                        .setPassword(options.getPassword().orElse(null))
                        .setAutoCommit(readOptions.getAutoCommit());

        if (readOptions.getFetchSize() != 0) {
            builder.setFetchSize(readOptions.getFetchSize());
        }
        final JdbcDialect dialect = options.getDialect();
        String query =
                dialect.getSelectFromStatement(
                        options.getTableName(),
                        getFieldNames(physicalRowDataType).toArray(new String[0]),
                        new String[0]);
        if (readOptions.getPartitionColumnName().isPresent()) {
            long lowerBound = readOptions.getPartitionLowerBound().get();
            long upperBound = readOptions.getPartitionUpperBound().get();
            int numPartitions = readOptions.getNumPartitions().get();
            builder.setParametersProvider(
                    new JdbcNumericBetweenParametersProvider(lowerBound, upperBound)
                            .ofBatchNum(numPartitions));
            query +=
                    " WHERE "
                            + dialect.quoteIdentifier(readOptions.getPartitionColumnName().get())
                            + " BETWEEN ? AND ?";
        }
        if (limit >= 0) {
            query = String.format("%s %s", query, dialect.getLimitClause(limit));
        }
        builder.setQuery(query);
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        builder.setRowConverter(dialect.getRowConverter(rowType));
        builder.setRowDataTypeInfo(
                runtimeProviderContext.createTypeInformation(physicalRowDataType));

        return InputFormatProvider.of(builder.build());
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public boolean supportsNestedProjection() {
        // JDBC doesn't support nested projection
        return false;
    }

    /*  @Override
        public void applyProjection(int[][] projectedFields, DataType producedDataType) {
            this.physicalRowDataType =null;
    //        this.physicalRowDataType = Projection.of(projectedFields).project(physicalRowDataType);
        }*/
    @Override
    public void applyProjection(int[][] projectedFields) {
        // TODO
        //       this.physicalRowDataType =
        // Projection.of(projectedFields).project(physicalRowDataType);
    }

    @Override
    public DynamicTableSource copy() {
        return new JdbcDynamicTableSource(options, readOptions, lookupOptions, physicalRowDataType);
    }

    @Override
    public String asSummaryString() {
        return "JDBC:" + dialectName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JdbcDynamicTableSource)) {
            return false;
        }
        JdbcDynamicTableSource that = (JdbcDynamicTableSource) o;
        return Objects.equals(options, that.options)
                && Objects.equals(readOptions, that.readOptions)
                && Objects.equals(lookupOptions, that.lookupOptions)
                && Objects.equals(physicalRowDataType, that.physicalRowDataType)
                && Objects.equals(dialectName, that.dialectName)
                && Objects.equals(limit, that.limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                options, readOptions, lookupOptions, physicalRowDataType, dialectName, limit);
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }
}
