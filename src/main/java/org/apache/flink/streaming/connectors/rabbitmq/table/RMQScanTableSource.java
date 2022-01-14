/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.rabbitmq.table;

import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.connectors.rabbitmq.config.JsonSimpleSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;

import org.apache.flink.table.descriptors.DescriptorProperties;
import java.util.*;


/** Defines the scan table source of RocketMQ. */
public class RMQScanTableSource implements ScanTableSource {

    private final DescriptorProperties properties;
    private final TableSchema schema;

    private final String host;
    private final String user;
    private final String password;
    private final String vhost;
    private final String queue;
    private final int port;


    private List<String> metadataKeys;

    public RMQScanTableSource(
            DescriptorProperties properties,
            TableSchema schema,
            String host,
            int port,
            String user,
            String password,
            String vhost,
            String queue) {
        this.properties = properties;
        this.schema = schema;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.vhost = vhost;
        this.queue = queue;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
            return SourceFunctionProvider.of(
                    new RMQSource(buildConfig(),queue,createJsonDeserializationSchema()),false);

    }


    @Override
    public DynamicTableSource copy() {
        RMQScanTableSource tableSource =
                new RMQScanTableSource(
                        properties,
                        schema,
                        host,
                        port,
                        user,
                        password,
                        vhost,
                        queue);
        tableSource.metadataKeys = metadataKeys;
        return tableSource;
    }

    @Override
    public String asSummaryString() {
        return RMQScanTableSource.class.getName();
    }



    private JsonSimpleSchema createJsonDeserializationSchema() {
        return new JsonSimpleSchema(schema);
    }


    private RMQConnectionConfig buildConfig(){
        return new RMQConnectionConfig.Builder()
                .setHost(host)
                .setPort(port)
                .setVirtualHost(vhost)
                .setUserName(user)
                .setPassword(password)
                .setAutomaticRecovery(true)
                .setNetworkRecoveryInterval(3000)
                .build();
    }

}
