package com.bonc.storm.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class C3P0ConnectionProvider implements ConnectionProvider {
	private static final Logger LOG = LoggerFactory.getLogger(DBManager.class);
	
	private Properties properties;
	
    private transient DataSource dataSource;

    public C3P0ConnectionProvider(Properties dbProperties) {
    	this.properties = dbProperties;
    }

	@Override
    public synchronized void prepare() {
        if(dataSource == null) {
    		
    		try {
    			
    			dataSource = BasicDataSourceFactory.createDataSource(properties);
    			
    			LOG.info("创建数据源成功");
    		} catch (Exception e) {
    			LOG.info("创建数据源失败："+e);
    		}
            
        }
    }
    
    @Override
    public Connection getConnection() {
        try {
            return this.dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
