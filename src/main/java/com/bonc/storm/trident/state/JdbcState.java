package com.bonc.storm.trident.state;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;

import com.bonc.storm.config.BooleanType;
import com.bonc.storm.config.FieldMapperConfiguration;
import com.bonc.storm.config.MessageType;
import com.bonc.storm.jdbc.ConnectionProvider;
import com.bonc.storm.jdbc.JdbcClient;
import com.bonc.storm.util.PropertityUtil;

public class JdbcState  implements State{
	
    private static final Logger LOG = LoggerFactory.getLogger(JdbcState.class);
    
    private FieldMapperConfiguration fieldMapperConfiguration;
    
	private ConnectionProvider connectionProvider;
	
	private JdbcClient jdbcClient;
	
	private int batchSize;
	
	private String insertStatement;
	
	
	public void prepare() {
		
		this.connectionProvider.prepare();
		
		this.fieldMapperConfiguration.prepare();
		
		this.jdbcClient = new JdbcClient(connectionProvider,fieldMapperConfiguration,batchSize);

		this.insertStatement = this.jdbcClient.getInsertStatement();
		
		if(insertStatement == null){
			throw new RuntimeException("插入语句为空insertStatement 为空："+insertStatement);
		}
		
		LOG.info("生成插入语句："+this.insertStatement);
		
	}
	
	
	public JdbcState withFieldMapperConfig(
			FieldMapperConfiguration fieldMapperConfiguration) {

			this.fieldMapperConfiguration = fieldMapperConfiguration;
		return this;
	}
	
	
	
	public JdbcState withConnectionProvider(ConnectionProvider connectionProvider){
		
		this.connectionProvider = connectionProvider;
		return this;
	}
	
	public JdbcState withBatchSize(int batchSize) {

		this.batchSize = batchSize;
		return this;
	}

	
	@Override
	public void beginCommit(Long txid) {
		LOG.debug("beginCommit is noop.");
	}

	@Override
	public void commit(Long txid) {
		LOG.debug("commit is noop.");
	}
	
	public void updateState(List<TridentTuple> tuples,
			TridentCollector collector) {
		
		
		this.jdbcClient.executeInsert(insertStatement,tuples);
		
	}



}
