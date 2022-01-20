package org.apache.flink.connector.jdbc.snc;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;

import java.util.List;
import java.util.stream.Collectors;

public class FlinkUtils15 {
    /**
     * 以下两个方法的组合，解决1.13中没有 context.getCatalogTable().getResolvedSchema().getPrimaryKeyIndexes() 1.13
     * DynamicTableFactory 类缺少getPrimaryKeyIndexes方法 default int[] getPrimaryKeyIndexes() { return
     * getCatalogTable().getResolvedSchema().getPrimaryKeyIndexes(); } 1.13
     * ResolvedSchema类缺少getPrimaryKeyIndexes方法 public int[] getPrimaryKeyIndexes() { final
     * List<String> columns = getColumnNames(); return getPrimaryKey()
     * .map(UniqueConstraint::getColumns) .map(pkColumns ->
     * pkColumns.stream().mapToInt(columns::indexOf).toArray()) .orElseGet(() -> new int[] {}); }
     *
     * @param resolvedSchema
     * @return
     */
    // context.getCatalogTable().getResolvedSchema().getPrimaryKeyIndexes(),
    public static int[] getPrimaryKeyIndexes(ResolvedSchema resolvedSchema) {
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
}
