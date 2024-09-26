package com.miya.mqtt.client.sdk.consume;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MqttConsumer {
    public void consume(String topic, String payload) {
        log.info("$$$$$ mqttConsumer|consume|topic=" + topic + "|payload=" + payload);
    }
}
