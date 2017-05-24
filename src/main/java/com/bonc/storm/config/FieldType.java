package com.bonc.storm.config;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlRootElement;


@XmlEnum
@XmlRootElement(name="type")
public enum FieldType {
	
	@XmlEnumValue("STRING")
	STRING, 
	
	@XmlEnumValue("INTEGER")
	INTEGER,
	
	@XmlEnumValue("LONG")
	LONG,
	
	@XmlEnumValue("DOUBLE")
	DOUBLE,
	
	@XmlEnumValue("FLOAT")
	FLOAT,
	
	@XmlEnumValue("BOOLEAN")
	BOOLEAN,
	
	@XmlEnumValue("DATE")
	DATE,
	
	@XmlEnumValue("TIMESTAMP")
	TIMESTAMP
	
}
