package com.bonc.storm.config;

import java.util.List;

import org.junit.Test;

import com.bonc.storm.util.PropertityUtil;

public class TestFieldMapperConfiguration {
	
	@Test
	public void testConfiguration() throws Exception{
		
		String fileName = "src/main/resources/log_data_access_field_mapper.xml";
		
		FieldMapperConfiguration configuration = FieldMapperConfiguration.getInstance(fileName);
		
		String splitChar = configuration.getSplitChar();
		
		System.out.println("SplitChar:"+splitChar);
		
		String unicode = "\\"+PropertityUtil.readUnicodeStr(splitChar);
		
		System.out.println("unicode:"+unicode);

		StringBuffer sb = new StringBuffer();
		
		sb.append("test01");
		
		sb.append(unicode);
		
		sb.append("test02");
		
		sb.append(unicode);
		
		sb.append("test03");
		
		
		System.out.println("length:"+sb.toString().split(unicode).length);
		
		
		
	}
	

	public void testConfigSplit() throws Exception{
		FieldMapperConfiguration configuration = FieldMapperConfiguration.getInstance("log_ftp_file_field_mapper.xml");
		
		String splitChar = configuration.getSplitChar();
		
		System.out.println("splitChar:"+splitChar);
		
//		String unicodeSplit = PropertityUtil.readUnicodeStr(splitChar);
		
		String str = "20151124|811|ITF_3Gdpi_mbl|135|847599cc-5d7f-4474-b37b-7c22c77de655|1|0|0|20151124234039618";
		
		String[] array = str.split(splitChar);
		
		for(int i = 0 ;i< array.length;i++){
			
			System.out.println(i+":"+array[i]);
			
		}
		
		
		
	}
	
}
