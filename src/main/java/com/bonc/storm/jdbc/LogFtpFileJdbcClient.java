package com.bonc.storm.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.trident.tuple.TridentTuple;

import com.bonc.storm.trident.mapper.JdbcMapper;

public class LogFtpFileJdbcClient {
	
	private static final Logger LOG = LoggerFactory.getLogger(LogFtpFileJdbcClient.class);

	private ConnectionProvider connectionProvider;
	
	private JdbcMapper mapper;
	
	private int batchSize;
	

	public LogFtpFileJdbcClient(ConnectionProvider connectionProvider,
			JdbcMapper mapper,int batchSize) {

		this.connectionProvider = connectionProvider;
		this.mapper = mapper;
		this.batchSize = batchSize;
	}



	public void executeInsert(String insertStatement, List<TridentTuple> tuples) {
		
		if(tuples == null || tuples.size() == 0){
			LOG.info("current tuples is null："+tuples);
			return ;
		}
		
		Connection conn = null;
		
		PreparedStatement pstmt = null;
		
		try {
			conn = connectionProvider.getConnection();
			
			conn.setAutoCommit(false);
			
			pstmt = conn.prepareStatement(insertStatement);
			
			int size = 0;
			
			for(TridentTuple tuple : tuples){
				
				List<Object> paramList = this.mapper.getColumns(tuple);
				if(paramList != null){
					
					for(int i = 0 ;i < paramList.size() ; i++){
						pstmt.setObject(i+1, paramList.get(i));
					}
					pstmt.addBatch();
					
					if(++size % batchSize == 0){
						
						int[] results = pstmt.executeBatch();
						
//						 if(Arrays.asList(results).contains(Statement.EXECUTE_FAILED)) {
//							 conn.rollback();
//							 throw new RuntimeException("failed at least one sql statement in the batch, operation rolled back.");
//						 }
						 
					}
					
				}
			}
			
			if(size > 0){
				pstmt.executeBatch();
				
				conn.commit();
				
				LOG.info("insert :"+size+" record successs");
			}
		} catch (SQLException e2) {
			try {
				conn.commit();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			LOG.info("execute insert statement error： " + insertStatement, e2);

		}finally{
			
			try {
				if(pstmt != null){
					pstmt.close();
				}
				
				if(conn != null){
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		
	}

}
