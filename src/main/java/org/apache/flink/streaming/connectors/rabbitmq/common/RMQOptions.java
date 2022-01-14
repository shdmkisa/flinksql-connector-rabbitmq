package org.apache.flink.streaming.connectors.rabbitmq.common;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * sql 模式所需参数
 */
public class RMQOptions {

    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host").stringType().noDefaultValue();

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port").intType().noDefaultValue();

    public static final ConfigOption<String> USER =
            ConfigOptions.key("user").stringType().noDefaultValue();

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password").stringType().noDefaultValue();

    public static final ConfigOption<String> VHOST =
            ConfigOptions.key("vhost").stringType().defaultValue("/");


        public static final ConfigOption<String> QUEUE =
            ConfigOptions.key("queue").stringType().defaultValue("/");


    public static final ConfigOption<Integer>  NETWORK_RECOVERY_INTERVAL =
            ConfigOptions.key("networkRecoveryInterval").intType().defaultValue(3000);

    public static final ConfigOption<Integer>  FRAME_MAX =
            ConfigOptions.key("requestedFrameMax").intType().noDefaultValue();

    public static final ConfigOption<Integer>  TIMEOUT =
            ConfigOptions.key("timeout").intType().noDefaultValue();

    public static final ConfigOption<Integer>  CHANNEL_MAX =
            ConfigOptions.key("requestedChannelMax").intType().noDefaultValue();

    public static final ConfigOption<Integer>  HEART_BEAT =
            ConfigOptions.key("requestedHeartbeat").intType().noDefaultValue();

    public static final ConfigOption<Boolean> AUTO_RECOVERY =
            ConfigOptions.key("automaticRecovery").booleanType().defaultValue(true);

    public static final ConfigOption<Boolean> TOPO_RECOVERY =
            ConfigOptions.key("topologyRecovery").booleanType().defaultValue(true);


}
