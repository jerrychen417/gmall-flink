package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//数据流：web/app -> Nginx -> 日志服务器 -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：Mock    -> Nginx -> Logger.sh -> Kafka(ZK) -> BaseLogApp Kafka(ZK)
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔5秒钟做一次CK
//        env.enableCheckpointing(5000L);
//        //2.2 指定CK的一致性语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //2.3 设置任务关闭的时候保留最后一次CK数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 指定从CK自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
//        //2.5 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"));
//        //2.6 设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //todo 2.消费kafka主题数据创建流
        DataStreamSource<String> kafkaStream = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_log", "base_log_app_210726"));

        //todo 3.转换为JSONObject对象并过滤数据
        OutputTag<String> outputTag = new OutputTag<String>("DirtyData") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjectStream = kafkaStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(outputTag, value);
                }
            }
        });
        //提取侧输出流并打印
        jsonObjectStream.getSideOutput(outputTag).print("Dirty>>>>>>>>>>>>>");

        //todo 4.新老用户校验  状态编程
        KeyedStream<JSONObject, String> keyedStream = jsonObjectStream.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagStream = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                //提取is_new标记
                String isNew = value.getJSONObject("common").getString("is_new");

                //判断isNew是否为1
                if ("1".equals(isNew)) {
                    //获取状态数据
                    String state = valueState.value();

                    //判断状态是否为NULL
                    if (state == null) {
                        //更新状态
                        valueState.update("0");
                    } else {
                        //修改标记
                        value.getJSONObject("common").put("is_new", "0");
                    }
                }

                return value;
            }
        });

        //todo 5.分流  侧输出流    页面日志  主流  启动日志&曝光日志  侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageStream = jsonObjWithNewFlagStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                //尝试获取start数据
                String start = value.getString("start");

                //判断start是否为null
                if (start != null) {
                    //启动日志
                    ctx.output(startTag, value.toJSONString());
                } else {
                    //页面日志
                    out.collect(value.toJSONString());

                    //尝试获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");

                    //判断是否为空
                    if (displays != null && displays.size() > 0) {
                        //获取页面ID
                        String pageId = value.getJSONObject("page").getString("page_id");

                        //遍历写出数据
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", pageId);
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        //todo 6.提取侧输出流数据并将数据写入对应的kafka主题
        DataStream<String> startStream = pageStream.getSideOutput(startTag);
        DataStream<String> displayStream = pageStream.getSideOutput(displayTag);

        pageStream.print();
        startStream.print();
        displayStream.print();

        pageStream.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        startStream.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayStream.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));


        env.execute("BaseLogApp");
    }
}