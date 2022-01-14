import com.rabbitmq.client.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.QueueingConsumer;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.connectors.rabbitmq.config.JsonSimpleSchema;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class Test {

    Connection connection;
    Channel channel;

   // @Before
    public void before() throws Exception{
        ConnectionFactory connectionFactory = new ConnectionFactory() ;
        connectionFactory.setHost("risen-cdh01");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("my_vhost");
        connectionFactory.setAutomaticRecoveryEnabled(true);
        connectionFactory.setNetworkRecoveryInterval(3000);
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("admin");
        if(connection == null && channel ==null){
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();
        }
    }

   // @After
    public void after() throws Exception{
        channel.close();
        connection.close();

    }



    @org.junit.Test
    public void test1() throws Exception{

        String exchangeName = "amq_fanout";
        String exchangeType = "fanout";
        String queueName = "test";
        String routingKey = "";    //不设置路由键
        channel.exchangeDeclare(exchangeName, exchangeType, true, false, false, null);
        channel.queueDeclare(queueName, true, false, false, null);
        channel.queueBind(queueName, exchangeName,routingKey);

        //durable 是否持久化消息
        QueueingConsumer consumer = new QueueingConsumer(channel);

        //参数：队列名称、是否自动ACK、Consumer
        channel.basicConsume(queueName, false,  consumer);
        //循环获取消息
        while(true){
            //获取消息，如果没有消息，这一步将会一直阻塞
            Delivery delivery = consumer.nextDelivery();
            String s = delivery.getEnvelope().toString();
            System.out.println(s);
            String message = new String(delivery.getBody());
            System.out.println("接收到消息："+message);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(),false);
        }
    }


    @org.junit.Test
    public void test2() throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("risen-cdh01")
                .setPort(5672)
                .setVirtualHost("my_vhost")
                .setUserName("admin")
                .setPassword("admin")
                .setAutomaticRecovery(true)
                .setNetworkRecoveryInterval(3000)
                .build();
        env.execute();

    }

    @org.junit.Test
    public void test3() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env, settings);

        tbEnv.executeSql("CREATE TABLE rabbitmq(" +
                "  id int," +
                "  name varchar," +
                "  c_date timestamp" +
                ") WITH (" +
                "  'connector' = 'rabbitmq-x'," +
                "  'host' = 'risen-cdh01'," +
                "  'port' = '5672'," +
                "   'user' = 'admin'," +
                "   'password'='admin',"+
                "   'vhost'='my_vhost',"+
                "   'queue'='test'"+
                ")");

        tbEnv.executeSql("CREATE TABLE mysql_source (" +
                "  id int," +
                "  name string," +
                "  c_date timestamp" +
                ") WITH (" +
                " 'connector' = 'jdbc'," +
                " 'url' = 'jdbc:mysql://risen-cdh01:3306/test'," +
                " 'username' = 'flinkx'," +
                " 'password' = 'risen@qazwsx123'," +
                " 'table-name' = 'mysql_test')");

        /*Table table = tbEnv.sqlQuery("select * from rabbitmq limit 100");
        DataStream<Row> rowDataStream = tbEnv.toAppendStream(table, Row.class);
        rowDataStream.print();*/
        TableResult tableResult = tbEnv.executeSql("insert into rabbitmq select * from mysql_source limit 10");
        tableResult.print();
        env.execute();
    }


    @org.junit.Test
    public void test4() throws Exception {

    }
}
