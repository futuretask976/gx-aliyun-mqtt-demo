package com.miya.mqtt.server.sdk;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.mqtt.server.ServerConsumer;
import com.alibaba.mqtt.server.callback.MessageListener;
import com.alibaba.mqtt.server.config.ChannelConfig;
import com.alibaba.mqtt.server.config.ConsumerConfig;
import com.alibaba.mqtt.server.model.MessageProperties;
import com.miya.mqtt.server.sdk.constant.MqttConsts;

public class MQTTConsumerDemo2 {
    public static void main(String[] args) throws Exception {
        /**
         * 此处参数内容为示意。接入点地址，购买实例，且配置完成后即可获取，接入点地址必须填写分配的域名，不得使用 IP 地址直接连接，否则可能会导致客户端异常。
         */
        String domain = MqttConsts.ENDPOINT;

        /**
         * 此处参数内容为示意。使用的协议和端口必须匹配，为5672。
         */
        int port = 5672;

        /**
         * 此处参数内容为示意。MQTT 实例 ID，购买后控制台获取
         */
        String instanceId = MqttConsts.INSTANCE_ID;

        /**
         * 此处参数内容为示意。账号 accesskey，从账号系统控制台获取
         */
        String accessKey = MqttConsts.ACCESS_KEY;

        /**
         * 此处参数内容为示意。账号 secretKey，从账号系统控制台获取，仅在Signature鉴权模式下需要设置
         */
        String secretKey = MqttConsts.ACCESS_KEY_SECRET;

        /**
         * 此处参数内容为示意。MQTT 消息的一级 topic，需要在控制台申请才能使用。
         * 如果使用了没有申请或者没有被授权的 topic 会导致鉴权失败，MQTT服务端会断开客户端连接。
         */
        String firstTopic = MqttConsts.DEMO_PARENT_TOPIC;

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
                System.out.println("Receive:" + msgId + "," + JSONObject.toJSONString(messageProperties) + "," + new String(payload));
            }
        });
    }
}
