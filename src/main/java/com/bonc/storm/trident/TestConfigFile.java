package com.bonc.storm.trident;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import com.bonc.storm.util.FileUtil;

public class TestConfigFile {
	
	
	public static void main(String[] args) throws Exception{
		
		if(args.length > 0){
			
			String filePath = args[0];
			
			InputStream inputStream = new FileInputStream(new File(filePath));
			
			String fileContent = FileUtil.getStringFromInputStream(inputStream);
			
			System.out.println("fileContent:"+fileContent);
			
			Properties pro = FileUtil.getProperteis(filePath);
			
			String configFilePath = (String)pro.get("fieldMapperFile");
			
			InputStream configInputStream = new FileInputStream(new File(configFilePath));
			
			String configContent = FileUtil.getStringFromInputStream(configInputStream);

			System.out.println("configContent:"+configContent);

			
		}else{
			//没有任何参数
			System.err.println("请输入参数。。。");
		}
		
	}
	
}
