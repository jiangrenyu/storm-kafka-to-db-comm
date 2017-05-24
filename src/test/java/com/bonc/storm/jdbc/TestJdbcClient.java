package com.bonc.storm.jdbc;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.bonc.storm.config.FieldMapperConfiguration;
import com.bonc.storm.util.FileUtil;


public class TestJdbcClient {
	
	public void testGetInsertStatement() throws Exception{
		ConnectionProvider connectionProvider = new C3P0ConnectionProvider(FileUtil.getProperteis(""));
		
		FieldMapperConfiguration fieldMapperConfiguration = FieldMapperConfiguration.getInstance("Field-Mapper.xml");
		
		JdbcClient client = new JdbcClient(connectionProvider,fieldMapperConfiguration,1000);
		
		System.out.println(client.getInsertStatement());
				
		
	}
	
	@Test
	public void testList(){
		
		List<String> list = new ArrayList<String>();
		list.add(null);
		list.add(null);
		list.add(null);
		
		System.out.println("size:"+list.size());
		
		
	}
	
	
}
