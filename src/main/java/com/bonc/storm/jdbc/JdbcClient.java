package com.bonc.storm.jdbc;

import com.bonc.storm.config.Field;
import com.bonc.storm.config.FieldMapperConfiguration;
import com.bonc.storm.config.FieldType;
import com.bonc.storm.config.MessageType;
import com.bonc.storm.util.DateUtil;
import com.bonc.storm.util.JsonUtil;
import net.sf.json.JSONObject;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class JdbcClient {
	
	private static final Logger LOG = LoggerFactory.getLogger(JdbcClient.class);

	private ConnectionProvider connectionProvider;
	
	private FieldMapperConfiguration fieldMapperConfiguration;
	
	private int batchSize;
	

	public JdbcClient(ConnectionProvider connectionProvider,
			FieldMapperConfiguration fieldMapperConfiguration,int batchSize) {

		this.connectionProvider = connectionProvider;
		this.fieldMapperConfiguration = fieldMapperConfiguration;
		this.batchSize = batchSize;
	}

	/**
	 * 根据字段映射配置 FieldMapperConfiguration 生成插入语句 
	 * @return
	 */
	public String getInsertStatement() {
		
		String tableName = this.fieldMapperConfiguration.getTableName();
		
		List<Field> fieldList = this.fieldMapperConfiguration.getFieldList();
		
		if(tableName != null && fieldList.size() > 0 ){
			
			StringBuffer namePart = new StringBuffer();
			namePart.append("insert into ");
			namePart.append(tableName);
			namePart.append("(");
			
			StringBuffer valuePart = new StringBuffer();
			for(Field field : fieldList){
				namePart.append(field.getFieldName());
				namePart.append(",");
				valuePart.append("?");
				valuePart.append(",");
			}
			
			namePart.deleteCharAt(namePart.length()-1);//去掉最后一个逗号
			
			valuePart.deleteCharAt(valuePart.length()-1);//去掉最后一个逗号
			
			namePart.append(") values (");
			
			namePart.append(valuePart);
			
			namePart.append(")");
			
			return namePart.toString();
		}
		
		return null;
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
				
				List<Object> paramList = getColumns(tuple);
				LOG.info("the parsed param is："+paramList);
				if(paramList != null){
					
					for(int i = 0 ;i < paramList.size() ; i++){
						pstmt.setObject(i+1, paramList.get(i));
						
					}
//					pstmt.setTimestamp(paramList.size()+1, new Timestamp(new Date().getTime()));
					pstmt.addBatch();
					
					if(++size % batchSize == 0){
						
						int[] results = pstmt.executeBatch();
						
//						 if(Arrays.asList(results).contains(Statement.EXECUTE_FAILED)) {
//							 conn.rollback();
//							 LOG.info("failed at least one sql statement in the batch, operation rolled back.");
//						 }
						 
					}
					
				}
			}
			
			if(size > 0){
				pstmt.executeBatch();
				
				conn.commit();
				
				LOG.info("成功插入:"+size+" 条");
			}
		} catch (SQLException e2) {
			
			try {
				conn.commit();
			} catch (SQLException e) {
				e.printStackTrace();
			}

			LOG.info("execute insert error： " + insertStatement, e2);

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


	private List<Object> getColumns_20151228(TridentTuple tuple) {

		String message = null;
		try {
			message = new String((byte[])tuple.get(0),"UTF-8");
//			LOG.info("receive message："+message);
		} catch (UnsupportedEncodingException e1) {
			LOG.info("transfer message "+tuple+" to String exception,",e1);
			return null;
		}

		//判断消息类型
		if(MessageType.JSON.equals(fieldMapperConfiguration.getHandleType())){
			//json消息
			JSONObject jsonObj = JsonUtil.parseStrToJson(message);

			List<Field> fieldList = this.fieldMapperConfiguration.getFieldList();

			if(jsonObj != null){
				//增加insert_time字段，用于测试效率使用
				jsonObj.accumulate("insert_time",DateUtil.formatDate(new Date(), "yyyyMMdd HH:mm:ss:S"));

				List<Object> resultList = new ArrayList<Object>();

				for(Field field :fieldList){
					resultList.add(getValue(field,jsonObj));
				}
				return resultList;
			}else{
				LOG.info("transfer message: "+message+" to JSONObject exception");
				return null;
			}

		}else if(MessageType.SPLIT.equals(fieldMapperConfiguration.getHandleType())){

			//拼接分隔符
//			String spliceChar = this.fieldMapperConfiguration.getSplitChar();
//
//			if(this.fieldMapperConfiguration.isUnicodeRegSplit()){
//				spliceChar = this.fieldMapperConfiguration.getRegSplit();
//			}

			//为测试使用加上一个insert_time字段
//			message = message+spliceChar+DateUtil.formatDate(new Date(), "yyyyMMdd HH:mm:ss:S");

			//字符消息
			List<Field> fieldList = this.fieldMapperConfiguration.getFieldList();

			String[] msgArray = message.split(this.fieldMapperConfiguration.getSplitChar());

			List<Object> resultList = new ArrayList<Object>();

			//实际配置的消息字段个数（已减去最后一个insert_time）
			int fieldLength = fieldList.size()-1;

			if(fieldLength > msgArray.length){
				//配置字段个数大于解析的消息个数，需增加fieldLength-msgArray.length个null
				int i =0 ;
				for(;i< msgArray.length;i++){
					resultList.add(transferField(fieldList.get(i),msgArray[i]));
				}
				while(i < fieldLength){
					resultList.add(null);
					i++;
				}
			}else{
				///配置字段个数小于等于解析的消息个数
				for(int j = 0 ; j <fieldLength;j++){
					resultList.add(transferField(fieldList.get(j),msgArray[j]));
				}
			}

			//增加最后一个insert_time
			resultList.add(transferField(fieldList.get(fieldList.size()-1),DateUtil.formatDate(new Date(), "yyyyMMdd HH:mm:ss:S")));


			return resultList;

		}

		return null;
	}


	private List<Object> getColumns(TridentTuple tuple) {
		
		String message = null;
		try {
			message = new String((byte[])tuple.get(0),"UTF-8");
			LOG.info("接收到消息："+message);
		} catch (UnsupportedEncodingException e1) {
			LOG.info("transfer message "+tuple+" to String exception,",e1);
			return null;
		}
		
		//判断消息类型
		if(MessageType.JSON.equals(fieldMapperConfiguration.getHandleType())){
			//json消息
			JSONObject jsonObj = JsonUtil.parseStrToJson(message);
			
			List<Field> fieldList = this.fieldMapperConfiguration.getFieldList();
			
			if(jsonObj != null){
				//增加insert_time字段，用于测试效率使用
				jsonObj.accumulate("insert_time",DateUtil.formatDate(new Date(), "yyyyMMdd HH:mm:ss:S"));

				List<Object> resultList = new ArrayList<Object>();
				
				for(Field field :fieldList){
					resultList.add(getValue(field,jsonObj));
				}
				return resultList;
			}else{
				LOG.info("transfer message: "+message+" to JSONObject exception");
				return null;
			}
			
		}else if(MessageType.SPLIT.equals(fieldMapperConfiguration.getHandleType())){
			
			//拼接分隔符
			String spliceChar = this.fieldMapperConfiguration.getSplitChar();
			
			if(this.fieldMapperConfiguration.isUnicodeRegSplit()){
				spliceChar = this.fieldMapperConfiguration.getRegSplit();
			}
			
			//为测试使用加上一个insert_time字段
			message = message+spliceChar+DateUtil.formatDate(new Date(), "yyyyMMdd HH:mm:ss:S");
			
			//字符消息
			List<Field> fieldList = this.fieldMapperConfiguration.getFieldList();
			
			String[] msgArray = message.split(this.fieldMapperConfiguration.getSplitChar());
			
			List<Object> resultList = new ArrayList<Object>();

			int fieldLength = fieldList.size();
			
			if(fieldLength > msgArray.length){
				//配置字段个数大于解析的消息个数，需增加fieldLength-msgArray.length个null
				int i =0 ;
				for(;i< msgArray.length;i++){
					resultList.add(transferField((Field)fieldList.get(i),msgArray[i]));
				}
				while(i < fieldLength){
					resultList.add(null);
					i++;
				}
			}else{
				///配置字段个数小于等于解析的消息个数
				for(int j = 0 ; j <fieldLength;j++){
					resultList.add(transferField(fieldList.get(j),msgArray[j]));
				}
			}
			
			return resultList;
		
		}
		
		return null;
	}

	
	/**
	 * 从jsonObj中解析出
	 * @param field
	 * @param jsonObj
	 * @return
	 */
	private Object getValue(Field field, JSONObject jsonObj) {
		
		String value = null;
		try {
			value = jsonObj.getString(field.getName());
			
			return transferField(field,value);
			
		} catch (Exception e1) {
			LOG.info("get field value exception,the field is："+field+" ,the record is："+jsonObj.toString(),e1);
			return null;
		}
		
	}
	
	
	/**
	 * 根据字段配置转换value
	 * @param field
	 * @param value
	 * @return
	 */
	private Object transferField(Field field, String value) {
		
		
		if("".equals(value)){
			return "";
		}
		
		
		try {
			value = value.trim();
			FieldType fieldType = field.getType();
			
			if(FieldType.STRING.equals(fieldType)){
				return value;
			}else if(FieldType.TIMESTAMP.equals(fieldType)){
				
				return new Timestamp(DateUtil.parseDate(value, field.getDateFormat()).getTime());

			}else if(FieldType.DOUBLE.equals(fieldType)){
				return Double.parseDouble(value);
			}else if(FieldType.INTEGER.equals(fieldType)){
				return Integer.parseInt(value);
			}else if(FieldType.DATE.equals(fieldType)){
				return DateUtil.parseDate(value, field.getDateFormat());
			}else if(FieldType.LONG.equals(fieldType)){
				return Long.parseLong(value);
			}else if(FieldType.FLOAT.equals(fieldType)){
				return Float.parseFloat(value);
			}else if(FieldType.BOOLEAN.equals(fieldType)){
				return Boolean.parseBoolean(value);
			}else{
				//不支持该类型
				LOG.info("dont't support type："+field.getType());
				return null;
			}
		} catch (Exception e) {
			LOG.info("transfer field value exception,the field is:"+field+",value is:"+value,e);
			return null;
		}
	}

}
