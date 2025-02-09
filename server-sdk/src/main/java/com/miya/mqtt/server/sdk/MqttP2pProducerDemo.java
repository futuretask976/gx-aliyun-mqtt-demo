package com.miya.mqtt.server.sdk;

import com.alibaba.mqtt.server.ServerProducer;
import com.alibaba.mqtt.server.callback.SendCallback;
import com.alibaba.mqtt.server.config.ChannelConfig;
import com.alibaba.mqtt.server.config.ProducerConfig;
import com.miya.mqtt.config.MqttConfig;
import com.miya.mqtt.server.sdk.constant.MqttConsts;

import java.nio.charset.StandardCharsets;

public class MqttP2pProducerDemo {
    public static void main(String[] args) throws Exception {
        /**
         * 此处参数内容为示意。接入点地址，购买实例，且配置完成后即可获取，接入点地址必须填写分配的域名，不得使用 IP 地址直接连接，否则可能会导致客户端异常。
         */
        String domain = MqttConfig.ENDPOINT;

        /**
         * 使用的协议和端口必须匹配，为5672。
         */
        int port = 5672;

        /**
         * 此处参数内容为示意。MQTT 实例 ID，购买后控制台获取
         */
        String instanceId = MqttConfig.INSTANCE_ID;

        /**
         * 此处参数内容为示意。账号 accesskey，从账号系统控制台获取
         */
        String accessKey = MqttConfig.ACCESS_KEY; // "accessKey";

        /**
         * 此处参数内容为示意。账号 secretKey，从账号系统控制台获取，仅在Signature鉴权模式下需要设置
         */
        String secretKey = MqttConfig.ACCESS_KEY_SECRET; // "secretKey";

        /**
         * 此处参数内容为示意。firstTopic是MQTT 消息的一级 topic，需要在控制台申请才能使用。
         * 如果使用了没有申请或者没有被授权的 topic 会导致鉴权失败，MQTT服务端会断开客户端连接。
         */
        String firstTopic = MqttConsts.DEMO_P2P_TOPIC_POSTFIX + MqttConsts.TOPIC_SEPERATOR + "GID_TEAMACHINE@@@MqttClientService";

        ChannelConfig channelConfig = new ChannelConfig();
        channelConfig.setDomain(domain);
        channelConfig.setPort(port);
        channelConfig.setInstanceId(instanceId);
        channelConfig.setAccessKey(accessKey);
        channelConfig.setSecretKey(secretKey);

        ServerProducer serverProducer = new ServerProducer(channelConfig, new ProducerConfig());
        serverProducer.start();

        for (int i = 0; i < 100; i++) {
            Thread.sleep(1000);
            String mqttTopic = firstTopic;
            System.out.println("MqttP2pProducerDemo#main topic=" + mqttTopic);
            serverProducer.sendMessage(mqttTopic, ("hello " + i).getBytes(StandardCharsets.UTF_8), new SendCallback() {
                @Override
                public void onSuccess(String msgId) {
                    System.out.println("SendSuccess " + msgId);
                }

                @Override
                public void onFail() {
                    System.out.println("SendFail ");
                }
            });
        }
    }
}
