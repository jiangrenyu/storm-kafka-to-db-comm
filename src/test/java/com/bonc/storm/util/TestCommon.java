package com.bonc.storm.util;

import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONObject;

import org.junit.Test;


public class TestCommon {
	
	@Test
	public void testSplit3(){
		
		Map<String,Integer> provMap = new HashMap<String,Integer>();
		
		
		
		int provNamePos = 2;
		
		int defaultProvId = -1;
		
		String[] paths = "/xx/beijng/test.txt/".split("/",-1);
		
		if(provNamePos >= 2 && provNamePos <= paths.length){
			String provName = paths[paths.length-provNamePos];
			System.out.println("provName:"+provName);
			Integer id = provMap.get(provName);
			
			defaultProvId = id != null ? id :defaultProvId;
		}
		
		System.out.println(defaultProvId);
				
	}
	
	
	
	public void testSplit2(){
		
		String str = "/a/b/s/s";
		
		String[] arrays = str.split("/",-1);
		
		for(int i = 0 ;i<arrays.length;i++){
			System.out.println(i+":"+arrays[i]);
		}
		
	}
	
	
	public void  testIndex(){
		
		Map<String,Integer> provMap = new HashMap<String,Integer>();
		
		Integer provId = provMap.get("xx");
		System.out.println(provId);
		
//		String inputMsg = "/beijing/xx.txt";
//		int lastIndex = inputMsg.lastIndexOf("/");
//		if(lastIndex != -1){
//			String dirPath = inputMsg.substring(0, lastIndex);
//			int dirIndex = dirPath.lastIndexOf("/");
//			if(dirIndex != -1 && dirPath.length() > (dirIndex+1)){
//				String desc = dirPath.substring(dirIndex+1);
//				int provId = provMap.get(desc);
//				System.out.println("desc:"+desc);
//				System.out.println("provId:"+provMap.get(desc));
//				System.out.println("provId:"+provId);
//			}
//		}
		
		
	}
	
	
	
	public void testJson(){
		
		JSONObject jsonObj = new JSONObject();
		jsonObj.accumulate("test", "tsfas");
		
		System.out.println(jsonObj.toString());
	}
	
	
	
	public void testSplit(){
		
		String splitChar ="\u0005";
		System.out.println("splitChar:"+splitChar);
		System.out.println("splitChar's length:"+splitChar.length());
		
		
		StringBuffer sb = new StringBuffer();
		sb.append("test1");
		sb.append(splitChar);
		sb.append("test2");
		sb.append(splitChar);
		sb.append("test3");
		
		System.out.println(sb.toString());
		
		System.out.println(sb.toString().replace(splitChar, ""));
		
		
	}
}
