package com.bonc.storm.trident.mapper;

import java.io.Serializable;
import java.util.List;

import org.apache.storm.tuple.ITuple;


public interface  JdbcMapper  extends Serializable{
	
	/**
	 * 从发送的元组中获取要插入数据库中的字段值，注意此时要确定字段类型，且返回字段顺序作为最后insert语句参数的顺序
	 * @param tuple
	 * @return
	 */
	List<Object> getColumns(ITuple tuple);
}
