package com.miya.mqtt.client.sdk;

import com.miya.mqtt.client.sdk.wrapper.ConnectionOptionWrapper;
import com.miya.mqtt.client.sdk.config.MqttConfig;
import com.miya.mqtt.client.sdk.concurrent.ExeService4Publish;
import com.miya.mqtt.client.sdk.constant.MqttConsts;
import com.miya.mqtt.client.sdk.util.MqttUtils;
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
public class MqttService implements InitializingBean {
    /**
     * MQTT客户端
     */
    private MqttClient mqttClient;

    @Override
    public void afterPropertiesSet() throws MqttException, NoSuchAlgorithmException, InvalidKeyException {
        if (mqttClient == null) {
            synchronized (MqttService.class) {
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
        String clientId = MqttConfig.CLIENT_ID;
        boolean cleanSession = false;

        MemoryPersistence memoryPersistence = new MemoryPersistence();
        mqttClient = new MqttClient("tcp://" + MqttConfig.ENDPOINT + ":1883",
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
        MqttService mqttService = new MqttService();
        mqttService.doInitMqttClient();
//        for (int i = 0; i < 15; i++) {
//            mqttService.sendTestMsg("testMsg=" + getNow());
//            Thread.sleep(1000 * 1);
//        }
        System.out.println("main exiting");
    }

    public static String getNow() {
        Date d = new Date(System.currentTimeMillis());
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        return format.format(d);
    }
}
