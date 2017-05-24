package com.bonc.storm.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.bonc.storm.jdbc.DBManager;

public class ProvService {
	
	private DBManager dbManager;
	
	public ProvService(DBManager dbManager) {

		this.dbManager = dbManager;
	}

	/**
	 * 从数据库中获取稽核规则,key为prov_desc，value为prov_id
	 * @return
	 */
	public Map<String,Integer> getProvInfo(){
		
		Connection connection = null;
		PreparedStatement statement = null;
		ResultSet rs = null;
		try {
			connection = dbManager.getConnection();
			statement = connection.prepareStatement("select prov_code,prov_desc,prov_desc2 from d_prov");
			rs = statement.executeQuery();
			
			Map<String,Integer> resultMap = new HashMap<String,Integer>();
			
			while(rs.next()){
				
				if(rs.getString("prov_desc") != null){
					
					resultMap.put(rs.getString("prov_desc").trim(), rs.getInt("prov_code"));
				}
				if(rs.getString("prov_desc2") != null){

					resultMap.put(rs.getString("prov_desc2").trim(), rs.getInt("prov_code"));
				}

			}
			
			return resultMap;
			
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			try {
				if(rs != null){
					rs.close();
				}
				if(statement != null){
					statement.close();
				}
				if(connection != null){
					connection.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}
	
}
