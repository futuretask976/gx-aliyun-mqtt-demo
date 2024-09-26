package com.miya.mqtt.server.sdk;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.mqtt.server.ServerConsumer;
import com.alibaba.mqtt.server.callback.MessageListener;
import com.alibaba.mqtt.server.config.ChannelConfig;
import com.alibaba.mqtt.server.config.ConsumerConfig;
import com.alibaba.mqtt.server.model.MessageProperties;
import com.miya.mqtt.server.sdk.constant.MqttConsts;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MqttConsumerDemo {
    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 3; i++) {
            MultiConsumer multiConsumer = new MultiConsumer("thread-" + i);
            multiConsumer.start();
        }
    }

    static class MultiConsumer extends Thread {
        private String threadName;

        public MultiConsumer(String threadName) {
            this.threadName = threadName;
        }

        @SneakyThrows
        @Override
        public void run() {
            /**
             * 设置云端SDK的接入点，请参见接入点说明中的云端SDK接入点格式。
             * 接入点地址必须填写分配的域名，不得使用IP地址直接连接，否则可能会导致服务端异常。
             */
            String domain = MqttConsts.ENDPOINT;

            /**
             * 使用的协议和端口必须匹配，该参数值固定为5672。
             */
            int port = 5672;

            /**
             * 您创建的云消息队列 MQTT 版的实例ID。
             */
            String instanceId = MqttConsts.INSTANCE_ID;

            /**
             * AccessKey ID，阿里云身份验证，在阿里云RAM控制台创建。
             * 阿里云账号AccessKey拥有所有API的访问权限，建议您使用RAM用户进行API访问或日常运维。
             * 强烈建议不要把AccessKey ID和AccessKey Secret保存到工程代码里，否则可能导致AccessKey泄露，威胁您账号下所有资源的安全。
             * 本示例以将AccessKey 和 AccessKeySecret 保存在环境变量为例说明。
             */
            String accessKey = MqttConsts.ACCESS_KEY; // System.getenv("MQTT_AK_ENV");
            /**
             * AccessKey Secret，阿里云身份验证，在阿里云RAM控制台创建。仅在签名鉴权模式下需要设置。
             */
            String secretKey = MqttConsts.ACCESS_KEY_SECRET; // System.getenv("MQTT_SK_ENV");

            /**
             * 云消息队列 MQTT 版消息的一级Topic，需要在控制台创建才能使用。
             * 由于云端SDK订阅消息一般用于云上应用进行消息汇总和分析等场景，因此，云端SDK订阅消息不支持设置子级Topic。
             * 如果使用了没有创建或者没有被授权的Topic会导致鉴权失败，服务端会断开客户端连接。
             */
            String firstTopic = MqttConsts.DEMO_PARENT_TOPIC; // "firstTopic";

            ChannelConfig channelConfig = new ChannelConfig();
            channelConfig.setDomain(domain);
            channelConfig.setPort(port);
            channelConfig.setInstanceId(instanceId);
            channelConfig.setAccessKey(accessKey);
            channelConfig.setSecretKey(secretKey);

            ServerConsumer serverConsumer = new ServerConsumer(channelConfig, new ConsumerConfig());
            serverConsumer.start();
            serverConsumer.subscribeTopic(firstTopic, new MessageListener() {
                @Override
                public void process(String msgId, MessageProperties messageProperties, byte[] payload) {
                    System.out.println(threadName + ": receive msg:" + msgId + "," + JSONObject.toJSONString(messageProperties) + "," + new String(payload));
                }
            });
            System.out.println("MultiConsumer#run " + threadName + " exiting");
        }
    }
}
