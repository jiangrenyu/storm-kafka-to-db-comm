package com.bonc.storm.config;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlRootElement;


@XmlEnum
@XmlRootElement(name="handleType")
public enum MessageType {
	
	@XmlEnumValue("JSON")
	JSON, 
	
	
	@XmlEnumValue("SPLIT")
	SPLIT
}
