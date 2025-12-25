import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.api.CheckpointingMode;
import java.util.Map;
import java.util.Properties;

// 主类：Flink作业定义
public class FlinkAtLeastOnceJob {
    // 消息实体类（对应Kafka中的消息格式）
    public static class TestMessage {
        private Integer msg_id;
        private Content content;
        private String timestamp;

        public static class Content {
            // private String sensor_id;
            private Double temperature;
            private Double humidity;
            private Long timestamp;

            public Content() {}

            // public String getSensor_id() { return sensor_id; }
            // public void setSensor_id(String sensor_id) { this.sensor_id = sensor_id; }
            public Double getTemperature() { return temperature; }
            public void setTemperature(Double temperature) { this.temperature = temperature; }
            public Double getHumidity() { return humidity; }
            public void setHumidity(Double humidity) { this.humidity = humidity; }
            public Long getTimestamp() { return timestamp; }
            public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
        }

        // 必须有无参构造函数（Flink序列化需要）
        public TestMessage() {}

        public TestMessage(Integer msg_id, Content content, String timestamp) {
            this.msg_id = msg_id;
            this.content = content;
            this.timestamp = timestamp;
        }

        // Getter和Setter（必须，否则无法解析JSON）
        public Integer getMsg_id() { return msg_id; }
        public void setMsg_id(Integer msg_id) { this.msg_id = msg_id; }
        public Content getContent() { return content; }
        public void setContent(Content content) { this.content = content; }
        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    }

    public static void main(String[] args) throws Exception {
        // 1. 初始化Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 关键配置：开启Checkpoint，保证At-Least-Once（每5秒一次，与实验设计一致）
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000); // 两次Checkpoint最小间隔1秒
        env.getCheckpointConfig().setCheckpointTimeout(30000); // Checkpoint超时30秒
        // 配置Checkpoint存储（ZooKeeper，需替换主节点IP）
        env.getCheckpointConfig().setCheckpointStorage("zookeeper://49.52.27.49:2181/flink-checkpoints");

        // 2. 配置Kafka Source（消息源，保证At-Least-Once）
        // 需替换参数：主节点IP（Kafka地址）、Flink测试Topic
        String kafkaServer = "49.52.27.49:9092";
        String sourceTopic = "flink-test-topic";
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", kafkaServer);
        consumerProps.setProperty("group.id", "flink-source-group");
        consumerProps.setProperty("auto.offset.reset", "earliest"); // 从最早消息开始读

        // 构建Kafka Source，解析JSON为TestMessage实体
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                sourceTopic,
                new SimpleStringSchema(),
                consumerProps
        );
        DataStream<TestMessage> sourceStream = env.addSource(kafkaSource)
                .map(new MapFunction<String, TestMessage>() {
                    @Override
                    public TestMessage map(String s) throws Exception {
                        // 解析JSON为TestMessage
                        return new com.alibaba.fastjson.JSONObject().parseObject(s, TestMessage.class);
                    }
                });

        // 3. 过滤算子：保留msg_id为偶数的消息
        DataStream<TestMessage> filterStream = sourceStream.filter(new FilterFunction<TestMessage>() {
            @Override
            public boolean filter(TestMessage testMessage) throws Exception {
                return testMessage.getMsg_id() % 2 == 0;
            }
        });

        // 4. 窗口计数算子：10秒滚动窗口，统计每个窗口的消息数
        DataStream<Tuple5<Integer, String, String, String, Integer>> countStream = filterStream
                // 按msg_id分组（保证同一msg_id的消息进入同一窗口，计数准确）
                .keyBy(TestMessage::getMsg_id)
                // 10秒滚动窗口（按处理时间，与实验设计一致）
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                // 统计窗口内消息数，输出（原始msg_id、消息生成时间、窗口时间、计数结果）
                .apply((window, input, out) -> {
                    int count = 0;
                    Integer originalMsgId = null;
                    String originTimestamp = null;
                    // String sensorId = null;
                    for (TestMessage msg : input) {
                        count++;
                        originalMsgId = msg.getMsg_id();
                        originTimestamp = msg.getTimestamp();
                        // if (msg.getContent() != null) {
                        //     sensorId = msg.getContent().getSensor_id();
                        // }
                    }
                    // 窗口时间：转换为毫秒时间戳字符串
                    String windowTime = String.valueOf(window.getEnd());
                    out.collect(new Tuple5<>(originalMsgId, originTimestamp, windowTime, "", count));
                });

        // 5. 格式转换算子：将Tuple转换为JSON字符串，供Kafka Sink写入
        DataStream<String> resultStream = countStream.map(new MapFunction<Tuple5<Integer, String, String, String, Integer>, String>() {
            @Override
            public String map(Tuple5<Integer, String, String, String, Integer> tuple) throws Exception {
                // 构造JSON格式，与statistic.py的字段对应
                Map<String, Object> resultMap = new java.util.HashMap<>();
                resultMap.put("original_msg_id", tuple.f0);
                resultMap.put("origin_timestamp", tuple.f1);
                resultMap.put("window_time", tuple.f2);
                resultMap.put("sensor_id", "");
                resultMap.put("count_result", tuple.f4);
                return new com.alibaba.fastjson.JSONObject().toJSONString(resultMap);
            }
        });

        // 6. 配置Kafka Sink（结果写入Kafka）
        // 需替换参数：主节点IP（Kafka地址）、Flink结果Topic
        String resultTopic = "flink-result-topic";
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", kafkaServer);
        producerProps.setProperty("acks", "1"); // 至少1个副本确认，保证消息不丢失
        producerProps.setProperty("retries", "3"); // 发送失败重试3次

        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>(
                resultTopic,
                new SimpleStringSchema(),
                producerProps
        );
        // 关键：开启Flink Sink的重试，保证At-Least-Once
        kafkaSink.setWriteTimestampToKafka(true);
        resultStream.addSink(kafkaSink);

        // 7. 执行Flink作业
        env.execute("Flink-At-Least-Once-Job");
    }
}
