package org.apache.flink.connector.nsq.table.descriptors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

@Internal
public class NsqValidator extends ConnectorDescriptorValidator {

    public static final String CONNECTOR_TYPE_VALUE_NSQ = "org/apache/flink/connector/nsq";

    public static final String NSQ_IP = "nsq.ip";

    public static final String NSQ_PORT = "nsq.port";

    public static final String NSQ_TOPIC = "nsq.topic";

    public static final String PARALLELISM = "parallelism";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
    }
}
