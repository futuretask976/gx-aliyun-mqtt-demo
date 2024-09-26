package com.miya.mqtt.server.sdk.constant;

public class MqttConsts {
    /**
     * MQ4IOT 实例 ID，购买后控制台获取
     */
    public static final String INSTANCE_ID = "post-cn-kvw3w72ds01";
    /**
     * 接入点地址，购买 MQ4IOT 实例，且配置完成后即可获取，接入点地址必须填写分配的域名，不得使用 IP 地址直接连接，否则可能会导致客户端异常。
     */
    public static final String ENDPOINT = "post-cn-kvw3w72ds01.mqtt.aliyuncs.com";

    /**
     * 账号 accesskey，从账号系统控制台获取
     * 阿里云账号AccessKey拥有所有API的访问权限，建议您使用RAM用户进行API访问或日常运维。
     * 强烈建议不要把AccessKey ID和AccessKey Secret保存到工程代码里，否则可能导致AccessKey泄露，威胁您账号下所有资源的安全。
     * 本示例以把AccessKey ID和AccessKey Secret保存在环境变量为例说明。运行本代码示例之前，请先配置环境变量MQTT_AK_ENV和MQTT_SK_ENV
     * 例如：export MQTT_AK_ENV=<access_key_id>
     *      export MQTT_SK_ENV=<access_key_secret>
     * 需要将<access_key_id>替换为已准备好的AccessKey ID，<access_key_secret>替换为AccessKey Secret。
     */
    public static final String ACCESS_KEY = "LTAI5t6hg6snjTBEddAP8tz8"; // System.getenv("MQTT_AK_ENV");

    /**
     * 账号 secretKey，从账号系统控制台获取，仅在Signature鉴权模式下需要设置
     */
    public static final String ACCESS_KEY_SECRET = "MammwnIOPrHe9AAO4CnaUJwmIG96Kc"; // System.getenv("MQTT_SK_ENV");

    /**
     * QoS参数代表传输质量，可选0，1，2，根据实际需求合理设置，具体参考 https://help.aliyun.com/document_detail/42420.html?spm=a2c4g.11186623.6.544.1ea529cfAO5zV3
     */
    public static final int QOS_LEVEL = 1;

    /**
     * MQ4IOT clientId，由业务系统分配，需要保证每个 tcp 连接都不一样，保证全局唯一，如果不同的客户端对象（tcp 连接）使用了相同的 clientId 会导致连接异常断开。
     * clientId 由两部分组成，格式为 GroupID@@@DeviceId，其中 groupId 在 MQ4IOT 控制台申请，DeviceId 由业务方自己设置，clientId 总长度不得超过64个字符。
     */
    public static final String CLIENT_ID = "GID_DEMO@@@mqttDemo1";

    /**
     * 超时时长
     */
    public static final int TIME_TO_WAIT = 5000;

    /**
     * topic 相关常量
     */
    public static final String DEMO_PARENT_TOPIC = "mqttdemo";
    public static final String DEMO_P2P_TOPIC_POSTFIX = "mqttdemo/p2p";

    /**
     * topic 分隔符
     */
    public static final String TOPIC_SEPERATOR = "/";
}
