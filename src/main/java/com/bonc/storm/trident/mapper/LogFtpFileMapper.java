package com.bonc.storm.trident.mapper;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.tuple.ITuple;

import com.bonc.storm.bean.Message;
import com.bonc.storm.util.DateUtil;

public class LogFtpFileMapper implements JdbcMapper {

	private static final Logger LOG = LoggerFactory.getLogger(LogFtpFileMapper.class);
	
	private Map<String, Integer> provMap;
	
	private int provNamePos;
	
	private int defaultProvId;
	
	public LogFtpFileMapper(Map<String, Integer> provMap, int provNamePos, int defaultProvId) {

		this.provMap = provMap;
		this.provNamePos = provNamePos;
		this.defaultProvId = defaultProvId;
	}

	@Override
	public List<Object> getColumns(ITuple tuple) {
		
		List<Object> columns = new ArrayList<Object>();
		
		try {
			Message inputMsg = (Message)tuple.getValue(0);
			
			String[] paths = inputMsg.getFilePath().split("/",-1);
			
			if(provNamePos >= 2 && provNamePos <= paths.length){
				Integer id = this.provMap.get(paths[paths.length-provNamePos]);
				defaultProvId = (id != null) ? id : -1;
			}
			
			columns.add(inputMsg.getFileName());
			columns.add(inputMsg.getOperateType());
			columns.add(inputMsg.getClientAddress());
			columns.add(inputMsg.getServerAddress());
			columns.add(inputMsg.getFtpAccount());
			columns.add(inputMsg.getFilePath());
			columns.add(inputMsg.getVersion());
			columns.add(inputMsg.getFileSize());
			columns.add(new Timestamp(DateUtil.parseDate(inputMsg.getBeginTime(), "yyyy-MM-dd HH:mm:ss").getTime()));
			columns.add(new Timestamp(DateUtil.parseDate(inputMsg.getEndTime(), "yyyy-MM-dd HH:mm:ss").getTime()));
			columns.add(defaultProvId);//省份ID
			columns.add(new Timestamp((new Date()).getTime()));//插入时间
			
			return columns;
		} catch (Exception e) {
			LOG.info("将消息映射为数据库字段失败："+e);
			return null;
		}
		
	}

}
