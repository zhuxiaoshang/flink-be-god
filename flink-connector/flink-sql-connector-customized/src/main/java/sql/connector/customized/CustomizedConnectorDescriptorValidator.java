package sql.connector.customized;

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

/**
 * 自定义connector描述符校验
 * with参数
 */
public class CustomizedConnectorDescriptorValidator extends ConnectorDescriptorValidator {
    public static final String CONNECTOR_JOB = "job";
    public static final String CONNECTOR_METRICS = "metrics";
    public static final String CONNECTOR_ADDRESS = "address";
    public static final String CONNECTOR_TYPE_VALUE_CUSTOMIZE = "customize";

    @Override
    public void validate(DescriptorProperties properties) {
        /**
         * 这里校验,比如配置中有address属性，是ip:port格式 可以对其进行校验
         */

        super.validate(properties);
    }
}
