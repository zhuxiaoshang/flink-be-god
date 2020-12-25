package formats.parquet;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetInputFormat;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.parquet.schema.MessageType;


/**
 * @author: zhushang
 * @create: 2020-12-22 19:25
 **/

public class ParquetRowDataInputFormat extends ParquetInputFormat<RowData> implements ResultTypeQueryable<RowData> {
	/**
	 * Read parquet files with given parquet file schema.
	 *
	 * @param path        The path of the file to read.
	 * @param messageType schema of parquet file
	 */
	protected ParquetRowDataInputFormat(Path path, MessageType messageType) {
		super(path, messageType);
	}

	/**
	 * Gets the data type (as a {@link TypeInformation}) produced by this function or input format.
	 *
	 * @return The data type produced by this function or input format.
	 */
	@Override
	public TypeInformation<RowData> getProducedType() {
		DataType dataType = TableSchema.builder().fields(getFieldNames(), TypeConversions.fromLegacyInfoToDataType(getFieldTypes())).build().toRowDataType();
		return (TypeInformation<RowData>) TypeConversions.fromDataTypeToLegacyInfo(dataType);
	}

	/**
	 * This ParquetInputFormat read parquet record as Row by default. Sub classes of it can extend this method
	 * to further convert row to other types, such as POJO, Map or Tuple.
	 *
	 * @param row row read from parquet file
	 * @return E target result type
	 */
	@Override
	public RowData convert(Row row) {
		DataType dataType = TableSchema.builder().fields(getFieldNames(), TypeConversions.fromLegacyInfoToDataType(getFieldTypes())).build().toRowDataType();
		DataStructureConverter converter = DataStructureConverters.getConverter(dataType);
		converter.open(getClass().getClassLoader());
		return (RowData) converter.toInternal(row);
	}
}
