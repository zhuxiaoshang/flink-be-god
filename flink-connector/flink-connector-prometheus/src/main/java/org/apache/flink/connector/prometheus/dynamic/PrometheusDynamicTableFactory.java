package org.apache.flink.connector.prometheus.dynamic;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.prometheus.PrometheusConnectorDescriptorValidator;
import org.apache.flink.connector.prometheus.PrometheusTableSink;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author: zhushang
 * @create: 2020-08-26 19:31
 **/

public class PrometheusDynamicTableFactory implements DynamicTableSinkFactory {
	public static final ConfigOption<String> ADDRESS = ConfigOptions.key("address")
		.stringType()
		.noDefaultValue()
		.withDescription("the prometheus's connect address.");
	public static final ConfigOption<Integer> PARALLELISM = ConfigOptions.key("parallelism")
		.intType()
		.defaultValue(-1)
		.withDescription("the prometheus's connect parallelism.");
	/**
	 * Creates a {@link DynamicTableSink} instance from a {@link CatalogTable} and additional context
	 * information.
	 *
	 * <p>An implementation should perform validation and the discovery of further (nested) factories
	 * in this method.
	 *
	 * @param context
	 */
	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		ReadableConfig tableOptions = helper.getOptions();
		helper.validateExcept("No");
		String address = tableOptions.get(ADDRESS);
		new PrometheusConnectorDescriptorValidator().validate(address);
		return new PrometheusDynamicTableSink(address, context.getCatalogTable().getSchema());
	}

	/**
	 * Returns a unique identifier among same factory interfaces.
	 *
	 * <p>For consistency, an identifier should be declared as one lower case word (e.g. {@code kafka}). If
	 * multiple factories exist for different versions, a version should be appended using "-" (e.g. {@code kafka-0.10}).
	 */
	@Override
	public String factoryIdentifier() {
		return "prometheus";
	}

	/**
	 * Returns a set of {@link ConfigOption} that an implementation of this factory requires in addition to
	 * {@link #optionalOptions()}.
	 *
	 * <p>See the documentation of {@link Factory} for more information.
	 */
	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(ADDRESS);
		return options;
	}

	/**
	 * Returns a set of {@link ConfigOption} that an implementation of this factory consumes in addition to
	 * {@link #requiredOptions()}.
	 *
	 * <p>See the documentation of {@link Factory} for more information.
	 */
	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(PARALLELISM);
		return options;
	}
}
