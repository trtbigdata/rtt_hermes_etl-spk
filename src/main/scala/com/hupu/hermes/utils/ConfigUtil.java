package com.hupu.hermes.utils;

import java.io.IOException;
import java.util.Properties;

public class ConfigUtil {

    private static Properties prop = new Properties();

    // 静态代码块，在类被加载的时候被执行，且只运行一次，优先于各种代码块以及构造函数
    static {
        try {
            // 加载config.properties配置文件，有几种方式，这种getClassLoader()是比较推荐的方式
            prop.load(ConfigUtil.class.getClassLoader().getResourceAsStream("config.properties"));
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    public static String getDealedProperty(String key) {
        String keyy = prop.getProperty(key);
        String value = prop.getProperty(keyy);
        return value;
    }

}
