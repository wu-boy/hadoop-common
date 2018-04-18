package com.wu.hadoop.common.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Properties例子
 * @author wusq
 * @date 2018-04-13
 */
public class PropertiesDemos {

	private static final String FILE_CONFIG = "/conf/config.properties";
	
	private static Map<String, String> configMap = new HashMap<>();

	// 需要初始化
	private static Configuration conf;

    public static String getStringByKey(String key){

    	String result = null;
    	if(configMap.containsKey(key)){
    		result = configMap.get(key);
    	}else{
    		//初始化configMap
    		Properties prop = new Properties();
    		try {
    			String uri = conf.get(FILE_CONFIG);
				FileSystem fs = FileSystem.get(URI.create(uri), conf);
				InputStream in = fs.open(new Path(uri));
				prop.load(in);
				Iterator<String> it = prop.stringPropertyNames().iterator();
				while(it.hasNext()){
					String k = it.next();
					configMap.put(k, prop.getProperty(k));
				}
				in.close();
				result = configMap.get(key);
			} catch (Exception e) {
				e.printStackTrace();
			}
    	}
    	return result;
    }

	public static Configuration getConf() {
		return conf;
	}

	public static void setConf(Configuration conf) {
		PropertiesDemos.conf = conf;
	}
    
}
