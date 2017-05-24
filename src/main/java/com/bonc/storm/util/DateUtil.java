package com.bonc.storm.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
	
	
	/***
	 * 以指定格式返回日期字符串
	 * @param date
	 * @param format
	 * @return
	 */
	public static String formatDate(Date date,String format){
		
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			
			return sdf.format(date);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		
	}
	
	/**
	 * 以指定格式解析字符串为日期
	 * @param dateStr		
	 * @param dateFormat
	 * @return
	 */
	public static Date parseDate(String dateStr, String dateFormat) {
		
		if(dateStr != null && dateFormat != null){
			try {
				SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
				
				return sdf.parse(dateStr);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}
	
	
	
}
