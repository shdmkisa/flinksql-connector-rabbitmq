/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.rabbitmq.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.*;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.rabbitmq.common.RMQOptions.*;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * Defines the {@link DynamicTableSourceFactory} implementation to create {@link
 * RMQScanTableSource}.
 */
public class RMQDynamicTableFactory implements DynamicTableSourceFactory,DynamicTableSinkFactory {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    @Override
    public String factoryIdentifier() {
        return "rabbitmq-x";
    }


    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(HOST);
        requiredOptions.add(USER);
        requiredOptions.add(PORT);
        requiredOptions.add(VHOST);
        requiredOptions.add(PASSWORD);
        requiredOptions.add(QUEUE);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(NETWORK_RECOVERY_INTERVAL);
        optionalOptions.add(AUTO_RECOVERY);
        optionalOptions.add(TOPO_RECOVERY);
        optionalOptions.add(TIMEOUT);
        return optionalOptions;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        transformContext(this, context);
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();
        Map<String, String> rawProperties = context.getCatalogTable().getOptions();
        Configuration configuration = Configuration.fromMap(rawProperties);
        String host = configuration.getString(HOST);
        String user = configuration.getString(USER);
        String password = configuration.getString(PASSWORD);
        String vhost = configuration.getString(VHOST);
        String queue = configuration.getString(QUEUE);
        int port = configuration.getInteger(PORT);

        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putTableSchema("schema", physicalSchema);
        return new RMQScanTableSource(
                descriptorProperties,
                physicalSchema,
                host,
                port,
                user,
                password,
                vhost,
                queue);
    }


    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        transformContext(this, context);
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();
        Map<String, String> rawProperties = context.getCatalogTable().getOptions();
        Configuration configuration = Configuration.fromMap(rawProperties);
        String host = configuration.getString(HOST);
        String user = configuration.getString(USER);
        String password = configuration.getString(PASSWORD);
        String vhost = configuration.getString(VHOST);
        String queue = configuration.getString(QUEUE);
        int port = configuration.getInteger(PORT);

        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putTableSchema("schema", physicalSchema);
        return new RMQDynamicTableSink(
                descriptorProperties,
                physicalSchema,
                host,
                port,
                user,
                password,
                vhost,
                queue);
    }



    private void transformContext(
            DynamicTableFactory factory, Context context) {
        Map<String, String> catalogOptions = context.getCatalogTable().getOptions();
        Map<String, String> convertedOptions =
                normalizeOptionCaseAsFactory(factory, catalogOptions);
        catalogOptions.clear();
        for (Map.Entry<String, String> entry : convertedOptions.entrySet()) {
            catalogOptions.put(entry.getKey(), entry.getValue());
        }
    }

    private Map<String, String> normalizeOptionCaseAsFactory(
            Factory factory, Map<String, String> options) {
        Map<String, String> normalizedOptions = new HashMap<>();
        Map<String, String> requiredOptionKeysLowerCaseToOriginal =
                factory.requiredOptions().stream()
                        .collect(
                                Collectors.toMap(
                                        option -> option.key().toLowerCase(), ConfigOption::key));
        Map<String, String> optionalOptionKeysLowerCaseToOriginal =
                factory.optionalOptions().stream()
                        .collect(
                                Collectors.toMap(
                                        option -> option.key().toLowerCase(), ConfigOption::key));
        for (Map.Entry<String, String> entry : options.entrySet()) {
            final String catalogOptionKey = entry.getKey();
            final String catalogOptionValue = entry.getValue();
            normalizedOptions.put(
                    requiredOptionKeysLowerCaseToOriginal.containsKey(
                                    catalogOptionKey.toLowerCase())
                            ? requiredOptionKeysLowerCaseToOriginal.get(
                                    catalogOptionKey.toLowerCase())
                            : optionalOptionKeysLowerCaseToOriginal.getOrDefault(
                                    catalogOptionKey.toLowerCase(), catalogOptionKey),
                    catalogOptionValue);
        }
        return normalizedOptions;
    }

}
