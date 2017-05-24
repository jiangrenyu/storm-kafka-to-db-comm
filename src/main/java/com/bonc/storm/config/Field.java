package com.bonc.storm.config;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAttribute;

public class Field implements Serializable{
	
	private String name;
	
	private FieldType type;
	
	private String fieldName;
	
	private String dateFormat;
	
	public Field(){}
	
	@XmlAttribute(name="name",required=false)
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@XmlAttribute(name="type",required=true)
	public FieldType getType() {
		return type;
	}

	public void setType(FieldType type) {
		this.type = type;
	}
	
	@XmlAttribute(name="fieldName",required=true)
	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}
	
	@XmlAttribute(name="dateFormat",required=false)
	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}
	
	@Override
	public String toString(){
		return "[name="+this.name+",type="+this.type+",fieldName="+this.fieldName+",dateFormat="+this.dateFormat+"]";
	}
	
}
