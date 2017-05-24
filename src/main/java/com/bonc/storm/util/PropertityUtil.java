package com.bonc.storm.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PropertityUtil {
	
    private static final Logger LOG = LoggerFactory.getLogger(PropertityUtil.class);
	
	/**
	 * 获取指定文件fileName中指定key的值
	 * @param fileName
	 * @param key
	 * @return
	 */
	public static String getValue(String fileName , String key){
		
		if(fileName != null && key != null){
			InputStream inputStream = PropertityUtil.class.getClassLoader().getResourceAsStream(fileName);
			
			Properties properties = new Properties();
			
			try {
				properties.load(inputStream);
				
				String val = properties.getProperty(key);
				if(val != null && val.length() > 0){
					return val.trim();
				}
			} catch (IOException e) {
				LOG.error("加载 "+fileName+"配置文件失败：",e);
			}finally{
				try {
					if(inputStream != null){
						inputStream.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return null;
	}
	
	
	
	public static String  readUnicodeStr(String unicodeStr){  
        StringBuilder buf = new StringBuilder();  
        //因为java转义和正则转义，所以u要这么写  
        String[] cc = unicodeStr.split("\\\\u");  
        for (String c : cc) {  
            if(c.equals(""))  
                continue;  
            int cInt = Integer.parseInt(c, 16);  
            char cChar = (char)cInt;  
            buf.append(cChar);  
        }  
        return buf.toString();  
    }
}
