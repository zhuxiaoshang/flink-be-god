package org.apache.flink.iceberg.compaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

/**
 * @author: zhushang
 * @create: 2021-04-02 14:30
 */
public class SparkCompaction {
    public static void main(String[] args) {
        TableIdentifier identifier = TableIdentifier.of(Namespace.of("db"), "table");

        Map<String, String> config = new HashMap<>();
        config.put("type", "iceberg");
        config.put("catalog-type", "hive");
        config.put("property-version", "1");
        config.put("warehouse", "warehouse");
        config.put("uri", "thrift://local:9083");
        config.put("io-impl", "org.apache.iceberg.aliyun.oss.OSSFileIO");
        config.put("oss.endpoint", "https://xxx.aliyuncs.com");
        config.put("oss.access.key.id", "key");
        config.put("oss.access.key.secret", "secret");

        sparkSession();
        HiveCatalog hiveCatalog = new HiveCatalog(new Configuration());
        hiveCatalog.initialize("iceberg_hive_catalog", config);

        Table table = hiveCatalog.loadTable(identifier);

        Actions.forTable(table).rewriteDataFiles().targetSizeInBytes(128 * 1024 * 1024).execute();

        Snapshot snapshot = table.currentSnapshot();
        if (snapshot != null) {
            table.expireSnapshots().expireOlderThan(snapshot.timestampMillis()).commit();
        }
    }

    private static void sparkSession() {
        SparkSession.builder()
                .master("local[*]")
                .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
                .config("spark.hadoop." + METASTOREURIS.varname, "localhost:9083")
                .config("spark.sql.warehouse.dir", "warehouse")
                .config("spark.executor.heartbeatInterval", "100000")
                .config("spark.network.timeoutInterval", "100000")
                .enableHiveSupport()
                .getOrCreate();
    }
}
