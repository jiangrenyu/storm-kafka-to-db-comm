package com.bonc.storm.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonc.storm.util.FileUtil;

public class DBManager {
	
	private static final Logger LOG = LoggerFactory.getLogger(DBManager.class);
	
	private  DataSource dataSource = null;
	
	private  Properties properties = null;
	
	
	public DBManager(String configPath) throws Exception{
		if(configPath != null){
			
			properties = FileUtil.getProperteis(configPath);
			
			dataSource = BasicDataSourceFactory.createDataSource(properties);
		}else{
			throw new Exception("can't found configPath:"+configPath);
		}
		
	}
	
	public Properties getProperties(){
		return this.properties;
	}
	
	/**
	 * 返回数据源
	 * @return
	 */
	public  DataSource getDataSource(){
		
		return dataSource;
	}
	
	/**
	 * 获取数据库连接
	 * @return
	 */
	public  Connection getConnection(){
		
		try {
			
			if(dataSource != null){
				return dataSource.getConnection();
			}
			
		} catch (SQLException e) {
			LOG.info("获取数据库连接失败："+e);
		}
		
		return null;
	}
	
	
	public static void closeConnection(Connection conn){
		
		try {
			if(conn != null){
				
				conn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	
}
