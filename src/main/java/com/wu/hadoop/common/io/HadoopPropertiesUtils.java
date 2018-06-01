package com.wu.hadoop.common.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.Properties;

/**
 * Hadoop全局配置工具类
 * @author wusq
 * @date 2018-05-17
 */
public class HadoopPropertiesUtils {

    /**
     * 初始化配置文件
     * @param configPath 配置文件目录
     * @param conf
     */
    public static void initProperties(String configPath, Configuration conf){
        Properties prop = new Properties();
        try {
            FileSystem fs = FileSystem.get(URI.create(configPath), conf);
            InputStream in = fs.open(new Path(configPath));
            prop.load(in);
            Iterator<String> it = prop.stringPropertyNames().iterator();
            while(it.hasNext()){
                String k = it.next();
                conf.set(k, prop.getProperty(k));
            }
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
