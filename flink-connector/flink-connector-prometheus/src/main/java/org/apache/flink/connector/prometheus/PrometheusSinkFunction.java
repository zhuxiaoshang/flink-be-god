package org.apache.flink.connector.prometheus;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PrometheusSinkFunction<T> extends RichSinkFunction<T> {
	public static final Logger LOG = LoggerFactory.getLogger(PrometheusSinkFunction.class);
	private static final long serialVersionUID = -7751331254238083935L;
	private final String address;
	private final String[] fieldNames;
	private final DataType[] dataTypes;
	private final PrometheusUtils prometheusUtils;
	private final int FIELDCOUNT = 5;

	public PrometheusSinkFunction(String address, String[] fieldNames, DataType[] dataTypes) {
		this.address = address;
		this.fieldNames = fieldNames;
		this.dataTypes = dataTypes;
		prometheusUtils = new PrometheusUtils();
		if (fieldNames.length < FIELDCOUNT) {
			LOG.warn("the table field {} is unsupported,required 'job' 'metrics' 'labels' 'groupingkeys' 'value'.", String.join(",", fieldNames));
			throw new FlinkRuntimeException(String.format("the table schema %s is unsupported,required 'job' 'metrics' 'labels' 'groupingkeys' 'value'.", String.join(",", fieldNames)));
		}
	}


	@Override
	public void invoke(T record, Context context) throws Exception {
		String job;
		String metrics;
		Map<String, String> labels;
		Map<String, String> groups;
		Double value;
		Row row;
		RowData rowData;
		if (record instanceof Row) {
			row = (Row) record;
			job = row.getField(0).toString();
			metrics = row.getField(1).toString();
			labels = (Map<String, String>) row.getField(2);
			groups = (Map<String, String>) row.getField(3);
			value = (Double) row.getField(4);
		} else if (record instanceof RowData) {
			rowData = (RowData) record;
			final LogicalType[] logicalTypes = Arrays.stream(dataTypes)
				.map(DataType::getLogicalType)
				.toArray(LogicalType[]::new);
			job = RowData.createFieldGetter(logicalTypes[0], 0).getFieldOrNull(rowData).toString();
			metrics = RowData.createFieldGetter(logicalTypes[1], 1).getFieldOrNull(rowData).toString();

			labels = mapData2JavaMap(rowData, logicalTypes, 2);
			groups = mapData2JavaMap(rowData, logicalTypes, 3);
			Object valueObject = RowData.createFieldGetter(logicalTypes[4], 4).getFieldOrNull(rowData);
			value = valueObject == null ? 0 : (Double) valueObject;
		} else {
			LOG.error("record type is unsupported.record is {}", record);
			throw new FlinkRuntimeException(String.format("record type is unsupported.record is %s", record));
		}

		try {
			prometheusUtils.pushMetrics(job, metrics, labels, groups, value, address);
		} catch (Exception e) {
			LOG.warn("sink to prometheus failed! value is {}", record, e);
		}
	}

	private Map<String, String> mapData2JavaMap(RowData rowData, LogicalType[] logicalTypes, int fieldIndex) {
		Map<String, String> javaMap = null;
		BinaryMapData labelMap = (BinaryMapData) RowData.createFieldGetter(logicalTypes[fieldIndex], fieldIndex).getFieldOrNull(rowData);
		if (labelMap != null) {
			javaMap = new HashMap<>();
			Map<?, ?> tmpMap = labelMap.toJavaMap(logicalTypes[fieldIndex].getChildren().get(0), logicalTypes[fieldIndex].getChildren().get(1));
			for (Map.Entry entry : tmpMap.entrySet()) {
				javaMap.put(entry.getKey().toString(), entry.getValue().toString());
			}
		}
		return javaMap;
	}
}
