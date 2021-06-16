package org.apache.flink.iceberg.compaction;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.actions.Actions;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: zhushang
 * @create: 2021-04-02 14:30
 */
public class FlinkCompaction {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "iceberg");
        properties.put("catalog-type", "hive");
        properties.put("property-version", "1");
        properties.put("warehouse", tool.get("warehouse"));
        properties.put("uri", tool.get("uri"));
        if (tool.has("oss.endpoint")) {
            properties.put("io-impl", "org.apache.iceberg.aliyun.oss.OSSFileIO");
            properties.put("oss.endpoint", tool.get("oss.endpoint"));
            properties.put("oss.access.key.id", tool.get("oss.access.key.id"));
            properties.put("oss.access.key.secret", tool.get("oss.access.key.secret"));
        }

        CatalogLoader loader =
                CatalogLoader.hive(tool.get("catalog"), new Configuration(), properties);
        Catalog catalog = loader.loadCatalog();

        TableIdentifier identifier =
                TableIdentifier.of(Namespace.of(tool.get("db")), tool.get("table"));

        Table table = catalog.loadTable(identifier);

        Actions.forTable(table)
                .rewriteDataFiles()
                .maxParallelism(5)
                .targetSizeInBytes(128 * 1024 * 1024)
                .execute();

        Snapshot snapshot = table.currentSnapshot();
        if (snapshot != null) {
            table.expireSnapshots().expireOlderThan(snapshot.timestampMillis()).commit();
        }
    }
}
