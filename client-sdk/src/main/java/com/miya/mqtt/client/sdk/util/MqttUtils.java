package com.miya.mqtt.client.sdk.util;

import com.miya.mqtt.config.MqttConfig;
import com.miya.mqtt.config.MqttServerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.Charset;
import java.security.*;
import java.util.*;

/**
 * Created by alvin on 17-3-29.
 */
@Slf4j
public class MqttUtils {
    public static String getTestTopic() {
        return MqttServerConfig.PARENT_TOPIC + MqttConfig.TOPIC_SEPERATOR + "console";
    }

    public static String getP2PTopic(String tenantCode, String machineCode) {
        return MqttServerConfig.P2P_TOPIC + MqttConfig.TOPIC_SEPERATOR + machineCode;
    }

    /**
     * 计算签名，参数分别是参数对以及密钥
     *
     * @param requestParams 参数对，即参与计算签名的参数
     * @param secretKey 密钥
     * @return 签名字符串
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     */
    public static String doHttpSignature(Map<String, String> requestParams,
            String secretKey) throws NoSuchAlgorithmException, InvalidKeyException {
        List<String> paramList = new ArrayList<String>();
        for (Map.Entry<String, String> entry : requestParams.entrySet()) {
            paramList.add(entry.getKey() + "=" + entry.getValue());
        }
        Collections.sort(paramList);
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < paramList.size(); i++) {
            if (i > 0) {
                sb.append('&');
            }
            sb.append(paramList.get(i));
        }
        return macSignature(sb.toString(), secretKey);
    }

    /**
     * @param text 要签名的文本
     * @param secretKey 阿里云MQ secretKey
     * @return 加密后的字符串
     * @throws InvalidKeyException
     * @throws NoSuchAlgorithmException
     */
    public static String macSignature(String text,
            String secretKey) throws InvalidKeyException, NoSuchAlgorithmException {
        Charset charset = Charset.forName("UTF-8");
        String algorithm = "HmacSHA1";
        Mac mac = Mac.getInstance(algorithm);
        mac.init(new SecretKeySpec(secretKey.getBytes(charset), algorithm));
        byte[] bytes = mac.doFinal(text.getBytes(charset));
        return new String(Base64.encodeBase64(bytes), charset);
    }
}
