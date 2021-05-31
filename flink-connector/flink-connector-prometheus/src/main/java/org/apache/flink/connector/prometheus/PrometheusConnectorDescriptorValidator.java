package org.apache.flink.connector.prometheus;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Validator for Prometheus Connector
 */
public class PrometheusConnectorDescriptorValidator extends ConnectorDescriptorValidator {
	public static final String CONNECTOR_ADDRESS = "connector.address";
	public static final String CONNECTOR_TYPE_VALUE_PROMETHEUS = "prometheus";
	public static final String PARALLELISM = "parallelism";
	public static final Pattern PATTERN = Pattern.compile("^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?):(6553[0-5]|655[0-2][0-9]|65[0-4][0-9][0-9]|6[0-4][0-9][0-9][0-9][0-9]|[1-5](\\d){4}|[1-9](\\d){0,3})$");

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		String address = properties.getString(CONNECTOR_ADDRESS);
		validate(address);
	}
	public void validate(String address) {
		Matcher matcher = PATTERN.matcher(address);
		if(!matcher.matches()){
			throw new ValidationException("the "+CONNECTOR_ADDRESS+" must be 'ip:port' format,but find "+address);
		}
	}
}
