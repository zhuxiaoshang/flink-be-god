package jdbc.dialect.customized;

import org.apache.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

/**
 * @author: zhushang
 * @create: 2020-09-01 15:54
 **/

public class CustomizedRowConverter extends AbstractJdbcRowConverter {
	@Override
	public String converterName() {
		return "ClickhouseSQL";
	}
	public CustomizedRowConverter(RowType rowType) {
		super(rowType);
	}
}
