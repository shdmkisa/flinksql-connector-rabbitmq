package org.apache.flink.streaming.connectors.rabbitmq.table;

import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.connectors.rabbitmq.config.JsonSimpleSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.types.RowKind;

public class RMQDynamicTableSink implements DynamicTableSink {

    private final DescriptorProperties properties;
    private final TableSchema schema;

    private final String host;
    private final String user;
    private final String password;
    private final String vhost;
    private final String queue;
    private final int port;


    public RMQDynamicTableSink(
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
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : changelogMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkFunctionProvider.of(new RMQSink<>(buildConfig(),queue,createJsonDeserializationSchema()));
    }

    @Override
    public DynamicTableSink copy() {
        return new RMQDynamicTableSink( properties,
                schema,
                host,
                port,
                user,
                password,
                vhost,
                queue);
    }

    @Override
    public String asSummaryString() {
        return RMQDynamicTableSink.class.getName();
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
