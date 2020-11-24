package sql.hive.commit.policy;

import org.apache.flink.hive.shaded.parquet.example.data.Group;
import org.apache.flink.hive.shaded.parquet.hadoop.ParquetFileReader;
import org.apache.flink.hive.shaded.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.flink.hive.shaded.parquet.hadoop.ParquetReader;
import org.apache.flink.hive.shaded.parquet.hadoop.ParquetWriter;
import org.apache.flink.hive.shaded.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.flink.hive.shaded.parquet.hadoop.example.GroupReadSupport;
import org.apache.flink.hive.shaded.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.flink.hive.shaded.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.flink.hive.shaded.parquet.hadoop.util.HadoopInputFile;
import org.apache.flink.hive.shaded.parquet.schema.MessageType;

import org.apache.flink.table.filesystem.PartitionCommitPolicy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * 小文件合并策略，代码搬自https://www.jianshu.com/p/1b3a67e89201
 */
public class ParquetFileMergingCommitPolicy implements PartitionCommitPolicy {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetFileMergingCommitPolicy.class);

    @Override
    public void commit(Context context) throws Exception {
        LOGGER.info("begin to merge files.partition path is {}.", context.partitionPath().toUri().toString());
//        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = new Configuration();
//        conf.set("fs.default.name", context.partitionPath().toUri().getHost());
        conf.set("fs.defaultFS", context.partitionPath().toUri().getHost());
        FileSystem fs = FileSystem.get(conf);
        String partitionPath = context.partitionPath().getPath();

        List files = listAllFiles(fs, new Path(partitionPath), "part-");
        LOGGER.info("{} files in path {}", files.size(), partitionPath);


        MessageType schema = getParquetSchema(files, conf);
        if (schema == null) {
            return;
        }
        LOGGER.info("Fetched parquet schema: {}", schema.toString());


        Path result = merge(partitionPath, schema, files, fs);
        LOGGER.info("Files merged into {}", result.toString());
    }


    private List<Path> listAllFiles(FileSystem fs, Path dir, String prefix) throws IOException {
        List result = new ArrayList<>();


        RemoteIterator dirIterator = fs.listFiles(dir, false);
        while (dirIterator.hasNext()) {
            LocatedFileStatus fileStatus = (LocatedFileStatus) dirIterator.next();
            Path filePath = fileStatus.getPath();
            if (fileStatus.isFile() && filePath.getName().startsWith(prefix)) {
                result.add(filePath);
            }
        }


        return result;
    }


    private MessageType getParquetSchema(List<Path> files, Configuration conf) throws IOException {
        if (files.size() == 0) {
            return null;
        }


        HadoopInputFile inputFile = HadoopInputFile.fromPath(files.get(0), conf);
        ParquetFileReader reader = ParquetFileReader.open(inputFile);
        ParquetMetadata metadata = reader.getFooter();
        MessageType schema = metadata.getFileMetaData().getSchema();


        reader.close();
        return schema;
    }


    private Path merge(String partitionPath, MessageType schema, List<Path> files, FileSystem fs) throws IOException {
        Path mergeDest = new Path(partitionPath + "/result-" + System.currentTimeMillis() + ".parquet");
//        GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
//        groupWriteSupport.setSchema(schema,fs.getConf());
//        ParquetWriter parquetWriter = new ParquetWriter(mergeDest,fs.getConf(), groupWriteSupport);
        ParquetWriter writer = ExampleParquetWriter.builder(mergeDest)
                .withType(schema)
                .withConf(fs.getConf())
                .withWriteMode(Mode.CREATE)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();
        LOGGER.info("dest file {}", mergeDest.toString());

        for (Path file : files) {
            ParquetReader reader = ParquetReader.builder(new GroupReadSupport(), file)
                    .withConf(fs.getConf())
                    .build();
            Group data;
            while ((data = (Group) reader.read()) != null) {
                writer.write(data);
            }
            reader.close();
        }
        LOGGER.info("data size is [{}]", writer.getDataSize());

        try {
            writer.close();
        } catch (Exception e) {
            LOGGER.error("flush failed!!!!", e);
        }

        if (!fs.exists(mergeDest)) {
            LOGGER.warn("Fuck! result file not exist.");
        }

        for (Path file : files) {
            fs.delete(file, false);
        }
        return mergeDest;
    }
}
