package com.bonc.storm.util;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsUtil {
	
	private static Logger LOG = LoggerFactory.getLogger(HdfsUtil.class);
	
//	private static Configuration conf = null;
	
	static {
		
//		String dfsName = PropertityUtil.getValue("fs.defaultFS");
//		
//		String jobTracker = PropertityUtil.getValue("mapred.job.tracker");
//		
//		if(dfsName != null && jobTracker != null){
//			
//			LOG.info("hafs配置如下，fs.default.name：{},mapred.job.tracker:{}",dfsName,jobTracker);
//			
//			conf = new Configuration();
//			
//			conf.set("fs.defaultFS", dfsName);
//			
//			conf.set("mapred.job.tracker", jobTracker);
//		}else{
//			
//			LOG.info("请检查 config.properties中 hafs 配置fs.default.name , mapred.job.tracker");
//
//		}
		
	}
	
//	public static  Configuration getConf(){
//		return conf;
//	}
	
	/**
	 * 重命名hdfs文件
	 * @param source	源文件名,全路径
	 * @param dest		目标文件名,全路径
	 * @return
	 */
	public static boolean rename(String source,String dest){
		
//		FileSystem  dfs = null;
//		try {
//			dfs = FileSystem.get(getConf());
//			if(source != null && dest != null){
//				dfs.rename(new Path(source), new Path(dest));
//				return true;
//			}
//		} catch (IOException e) {
//			LOG.error("重命名文件 "+source+" 为 "+dest+"失败",e);
//		}finally{
//			try {
//				if(dfs != null){
//					dfs.close();
//				}
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//			
//		}
		return false;
	}
	
}
