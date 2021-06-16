package org.apache.flink.iceberg.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformations;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: zhushang
 * @create: 2021-03-10 00:03
 */
public class Mysql2Iceberg {
    private static final Schema SCHEMA =
            new Schema(
                    Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "name", Types.StringType.get()),
                    Types.NestedField.optional(3, "age", Types.IntegerType.get()),
                    Types.NestedField.optional(4, "sex", Types.StringType.get()));
    private static final String HIVE_CATALOG = "iceberg_hive_catalog";
    private static final String HADOOP_CATALOG = "iceberg_hadoop_catalog";

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(60 * 1000L);
        checkpointConfig.setMinPauseBetweenCheckpoints(60 * 1000L);
        checkpointConfig.setTolerableCheckpointFailureNumber(10);
        checkpointConfig.setCheckpointTimeout(120 * 1000L);
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<RowData> src = env.addSource(getMysqlCdc(parameterTool));

        //        src.print().setParallelism(1);
        if ("hadoop".equals(parameterTool.get("catalog"))) {
            icebergSink_hadoop(src, parameterTool);
        } else {
            icebergSink_hive(src, parameterTool);
        }

        env.execute();
    }

    private static void icebergSink_hive(DataStreamSource<RowData> src, ParameterTool tool) {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "iceberg");
        properties.put("catalog-type", "hive");
        properties.put("property-version", "1");
        properties.put("warehouse", tool.get("warehouse"));
        properties.put("uri", tool.get("uri"));

        CatalogLoader catalogLoader =
                CatalogLoader.hive(HIVE_CATALOG, new Configuration(), properties);

        icebergSink(src, tool, catalogLoader);
    }

    private static void icebergSink_hadoop(DataStream<RowData> src, ParameterTool tool) {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "iceberg");
        properties.put("catalog-type", "hadoop");
        properties.put("property-version", "1");
        properties.put("warehouse", tool.get("warehouse"));

        CatalogLoader catalogLoader =
                CatalogLoader.hadoop(HADOOP_CATALOG, new Configuration(), properties);

        icebergSink(src, tool, catalogLoader);
    }

    private static void icebergSink(DataStream input, ParameterTool tool, CatalogLoader loader) {
        Catalog catalog = loader.loadCatalog();

        TableIdentifier identifier =
                TableIdentifier.of(Namespace.of(tool.get("hive_db")), tool.get("hive_table"));

        Table table;
        if (catalog.tableExists(identifier)) {
            table = catalog.loadTable(identifier);
        } else {
            table =
                    catalog.buildTable(identifier, SCHEMA)
                            .withPartitionSpec(PartitionSpec.unpartitioned())
                            .create();
        }
        // need to upgrade version to 2,otherwise 'java.lang.IllegalArgumentException: Cannot write
        // delete files in a v1 table'
        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));

        TableLoader tableLoader = TableLoader.fromCatalog(loader, identifier);

        FlinkSink.forRowData(input)
                .table(table)
                .tableLoader(tableLoader)
                .equalityFieldColumns(Arrays.asList("id"))
                .writeParallelism(1)
                .build();
    }

    private static SourceFunction getMysqlCdc(ParameterTool tool) {
        TableSchema schema =
                TableSchema.builder()
                        .add(TableColumn.physical("id", DataTypes.INT()))
                        .add(TableColumn.physical("name", DataTypes.STRING()))
                        .add(TableColumn.physical("age", DataTypes.INT()))
                        .add(TableColumn.physical("sex", DataTypes.STRING()))
                        .build();
        RowType rowType = (RowType) schema.toRowDataType().getLogicalType();
        DebeziumDeserializationSchema deserialer =
                new RowDataDebeziumDeserializeSchema(
                        rowType,
                        createTypeInfo(schema.toRowDataType()),
                        (rowData, rowKind) -> {},
                        ZoneId.of("Asia/Shanghai"));
        SourceFunction<RowData> sourceFunction =
                MySQLSource.<RowData>builder()
                        .hostname(tool.get("host"))
                        .port(3306)
                        .databaseList(tool.get("db")) // monitor all tables under inventory database
                        .username(tool.get("user"))
                        .password(tool.get("password"))
                        .tableList(tool.get("db") + "." + tool.get("table"))
                        .deserializer(deserialer) // converts SourceRecord to RowData
                        //                .deserializer(new StringDebeziumDeserializationSchema())
                        .build();
        return sourceFunction;
    }

    private static TypeInformation<RowData> createTypeInfo(DataType producedDataType) {
        final DataType internalDataType =
                DataTypeUtils.transform(producedDataType, TypeTransformations.TO_INTERNAL_CLASS);
        return (TypeInformation<RowData>)
                TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(internalDataType);
    }
}
