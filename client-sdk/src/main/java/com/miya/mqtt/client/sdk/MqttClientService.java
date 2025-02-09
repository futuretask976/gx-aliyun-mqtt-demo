package com.miya.mqtt.client.sdk;

import com.miya.mqtt.client.sdk.wrapper.ConnectionOptionWrapper;
import com.miya.mqtt.client.sdk.concurrent.ExeService4Publish;
import com.miya.mqtt.client.sdk.constant.MqttConsts;
import com.miya.mqtt.client.sdk.util.MqttUtils;
import com.miya.mqtt.config.MqttClientConfig;
import com.miya.mqtt.config.MqttConfig;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

@Component
public class MqttClientService implements InitializingBean {
    /**
     * MQTT客户端
     */
    private org.eclipse.paho.client.mqttv3.MqttClient mqttClient;

    @Override
    public void afterPropertiesSet() throws MqttException, NoSuchAlgorithmException, InvalidKeyException {
        if (mqttClient == null) {
            synchronized (MqttClientService.class) {
                if (mqttClient == null) {
                    doInitMqttClient();
                }
            }
        }
    }

    public void sendTestMsg(String payload) {
        ExeService4Publish.getExecutorService().submit(() -> {
            MqttMessage message = new MqttMessage(payload.getBytes());
            message.setQos(MqttConfig.QOS_LEVEL);
            try {
                mqttClient.publish(MqttUtils.getTestTopic(), message);
                System.out.println("messageSent topic=" + MqttUtils.getTestTopic() + ", message=" + message);
            } catch (MqttException e) {
                e.printStackTrace();
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        });
    }

    public void sendP2PMsgByTenant(String tenantCode, String machineCode, String payload) {
        ExeService4Publish.getExecutorService().submit(() -> {
            MqttMessage message = new MqttMessage(payload.getBytes());
            message.setQos(MqttConfig.QOS_LEVEL);
            try {
                mqttClient.publish(MqttUtils.getP2PTopic(tenantCode, machineCode), message);
            } catch (MqttException e) {
                e.printStackTrace();
            }
        });
    }

    public void onDestroy() {
        try {
            mqttClient.disconnect();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private void doInitMqttClient() throws MqttException, NoSuchAlgorithmException, InvalidKeyException {
        String clientId = MqttClientConfig.CLIENT_ID;
        boolean cleanSession = false;

        MemoryPersistence memoryPersistence = new MemoryPersistence();
        mqttClient = new org.eclipse.paho.client.mqttv3.MqttClient("tcp://" + MqttConfig.ENDPOINT + ":1883",
                clientId, memoryPersistence);
        // 客户端设置好发送超时时间，防止无限阻塞
        mqttClient.setTimeToWait(MqttConfig.TIME_TO_WAIT);

        // 设置订阅
        mqttClient.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                // 客户端连接成功后就需要尽快订阅需要的 topic
                try {
                    String[] topicFilters = getTopicFilters();
                    for (String topicFilter : topicFilters) {
                        System.out.println("topicFilter=" + topicFilter);
                    }
                    int[] qos = getQos();
                    mqttClient.subscribe(topicFilters, qos);
                } catch (MqttException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void connectionLost(Throwable throwable) {
                throwable.printStackTrace();
            }

            @Override
            public void messageArrived(String topic, MqttMessage mqttMessage) {
                System.out.println("messageArrived topic=" + topic + ", payload=" + new String(mqttMessage.getPayload()));
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
                System.out.println("mqtt send success: " + iMqttDeliveryToken.getTopics()[0]);
            }
        });

        //Map<String, String> tokenData = new HashMap<String, String>();
        //tokenData.put("RW", "LzMT+XLFl5s/YWJ/MlDz4t/Lq5HC1iGU1P28HAMaxYzmBSHQsWXgdISJ1ZJ+2cxa/G8rPlXN3FGNjdn2bmgs/9YWdl0jS//fbJGYgJWUr5piesdvDY0i8V1ENftD3MvAgqy5OII7Bl/vooOIjF1CZWKWxif/KoHAERkHVygfiMfiqhAzYZGKHyQgoJWL8b3AwXp1M60Hjp+oi27VLLD/3EnVgGDRZD+d4M0JsnqDQCfWhAVZ1XCLGbqkxBIPOiLA9GMAmYMUCAM477R+Sg86UHi5UJOKP4uydqvyabOF170S2wbZObWHENkwvkpgh2KXQXrpfocrsr2mVqt1V+/oIsCAJgNV4tX7ybNe0hsYT9RKUCfixKKGGS+M3KWrnb8z");
        //ConnectionOptionWrapper connectionOptionWrapper = new ConnectionOptionWrapper(MqttConfig.INSTANCE_ID, MqttConfig.ACCESS_KEY, clientId, tokenData);
        ConnectionOptionWrapper connectionOptionWrapper = new ConnectionOptionWrapper(MqttConfig.INSTANCE_ID,
                MqttConfig.ACCESS_KEY, MqttConfig.ACCESS_KEY_SECRET,
                clientId, cleanSession);
        mqttClient.connect(connectionOptionWrapper.getMqttConnectOptions());
        System.out.println("doInitMqttClient exiting");
    }

    private String[] getTopicFilters() {
        return new String[]{
                MqttConsts.TENANT_PARENT_TOPIC + MqttConsts.TOPIC_SEPERATOR + "broadcast"
        };
    }

    private int[] getQos() {
        return new int[] {
                MqttConfig.QOS_LEVEL
        };
    }

    public static void main(String args[]) throws Exception {
        MqttClientService mqttPublisher = new MqttClientService();
        mqttPublisher.doInitMqttClient();
        //for (int i = 0; i < 1000; i++) {
        //    mqttPublisher.sendTestMsg("testMsg=" + getNow());
        //    Thread.sleep(1000);
        //}
        //System.out.println("main exiting");
    }

    public static String getNow() {
        Date d = new Date(System.currentTimeMillis());
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        return format.format(d);
    }
}
