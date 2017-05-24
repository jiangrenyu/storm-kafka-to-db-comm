package com.bonc.storm.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonc.storm.util.PropertityUtil;



@XmlRootElement(name="fieldMapperConfiguration")
public class FieldMapperConfiguration implements Serializable{
	
	private static final Logger LOG = LoggerFactory.getLogger(FieldMapperConfiguration.class);
	
	private MessageType handleType;
	
	private String splitChar;
	
	private String tableName;
	
	private BooleanType isUnicode;
	
	/**
	 * 分隔符是否是unicode编码，且是正则中的特殊字符，默认为false
	 */
	private boolean isUnicodeRegSplit = false;
	
	/**
	 * 分隔符是unicode编码，且是正则中的特殊字符
	 */
	private String regSplit;
	
	private List<Field> fieldList = new ArrayList<Field>(0);
	
	public	static FieldMapperConfiguration getInstance(String fileName) throws Exception{
		
		Reader reader = null;
		
		if(fileName != null){
			//new FileInputStream(fileName)
			
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(fileName)),"UTF-8"));
//			reader = new BufferedReader(new InputStreamReader(FieldMapperConfiguration.class.getClassLoader().getResourceAsStream(fileName)));
			
			JAXBContext context = JAXBContext.newInstance(new Class[] {FieldMapperConfiguration.class, MessageType.class, Field.class, FieldType.class});
			Unmarshaller unmarshaller = context.createUnmarshaller();
						
			// Initialize configuration
			FieldMapperConfiguration fieldMapperConfiguration  = (FieldMapperConfiguration) unmarshaller.unmarshal(reader);
			
			return fieldMapperConfiguration;
		}else{
			throw new Exception("can't found file:"+fileName);
		}
		
	}
	
	private FieldMapperConfiguration(){
		
	}
	
	
	@XmlAttribute(name="handleType",required=true)
	public MessageType getHandleType() {
		return handleType;
	}


	public void setHandleType(MessageType handleType) {
		this.handleType = handleType;
	}

	@XmlAttribute(name="splitChar",required=false)
	public String getSplitChar() {
		return splitChar;
	}


	public void setSplitChar(String splitChar) {
		this.splitChar = splitChar;
	}

	@XmlElement(name="field", nillable=false)
	public List<Field> getFieldList() {
		return fieldList;
	}


	public void setFieldList(List<Field> fieldList) {
		this.fieldList = fieldList;
	}
	
	@XmlAttribute(name="tableName",required=true)
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	@XmlAttribute(name="isUnicode",required=false)
	public BooleanType getIsUnicode() {
		return isUnicode;
	}

	public void setIsUnicode(BooleanType isUnicode) {
		this.isUnicode = isUnicode;
	}
	
	
	public boolean isUnicodeRegSplit() {
		return isUnicodeRegSplit;
	}

	public void setUnicodeRegSplit(boolean isUnicodeRegSplit) {
		this.isUnicodeRegSplit = isUnicodeRegSplit;
	}

	public String getRegSplit() {
		return regSplit;
	}

	public void setRegSplit(String regSplit) {
		this.regSplit = regSplit;
	}

	public void prepare() {
		
		if(MessageType.SPLIT.equals(getHandleType()) && (getSplitChar() == null || getSplitChar().length() == 0)){
			//为字符分隔消息，需判断分隔符是否配置
			LOG.info("当消息为字符分隔时，分隔符不能为空："+getSplitChar());
			throw new RuntimeException("当消息为字符分隔时，分隔符不能为空："+getSplitChar());
		}else if(MessageType.SPLIT.equals(getHandleType()) && BooleanType.TRUE.equals(getIsUnicode())){
			//分隔符为unicode字符
			
			String unicodeSplitChar = PropertityUtil.readUnicodeStr(getSplitChar());
			if(".$|^?*+".indexOf(unicodeSplitChar.charAt(0)) != -1){
				this.setSplitChar("\\"+unicodeSplitChar);
				//分隔符是unicode编码且是正则中的特殊字符
				isUnicodeRegSplit = true;
				
				regSplit = unicodeSplitChar;
				
			}else{
				this.setSplitChar(unicodeSplitChar);
			}
			
		}
		
	}
	
	
	
}
