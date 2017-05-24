package com.bonc.storm.util;

import net.sf.json.JSONObject;

public class JsonUtil {
	
	/**
	 * 将java对象转换为json字符串
	 * @param obj
	 * @return
	 */
	public static String objectToString(Object obj){
		
		try {
			return JSONObject.fromObject(obj).toString();
		} catch (Exception e) {
			e.printStackTrace();
			return "";
		}
	}
	
	/**
	 * 将字符串转换为JSONObject
	 * @param str
	 * @return
	 */
	public static JSONObject parseStrToJson(String str){
		try {
			return JSONObject.fromObject(str);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		
	}
	
}
