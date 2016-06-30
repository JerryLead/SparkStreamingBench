package tcse.kafka.test;

import tcse.join.test.JoinConfig;

/**
 * Created by DuanSky on 2016/6/24.
 */
public class Config {

    public static final String topics = JoinConfig.topics();
    public static final String zookeeper =JoinConfig.zkQuorum();
    public static final String brokers = JoinConfig.brokers();
    public static final String group = "duansky-1";
    public static final int count = 100;

}
