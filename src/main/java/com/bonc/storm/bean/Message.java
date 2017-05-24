package com.bonc.storm.bean;

import java.io.Serializable;

public class Message implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8299457523312128769L;

	private String 	fileName;
	
	private int operateType;
	
	private String clientAddress;
	
	private String serverAddress;

	private String ftpAccount;
	
	private String filePath;
	
	private String version;
	
	private double fileSize;
	
	private String beginTime;
	
	private String endTime;

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public int getOperateType() {
		return operateType;
	}

	public void setOperateType(int operateType) {
		this.operateType = operateType;
	}

	public String getClientAddress() {
		return clientAddress;
	}

	public void setClientAddress(String clientAddress) {
		this.clientAddress = clientAddress;
	}

	public String getServerAddress() {
		return serverAddress;
	}

	public void setServerAddress(String serverAddress) {
		this.serverAddress = serverAddress;
	}

	public String getFtpAccount() {
		return ftpAccount;
	}

	public void setFtpAccount(String ftpAccount) {
		this.ftpAccount = ftpAccount;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public double getFileSize() {
		return fileSize;
	}

	public void setFileSize(double fileSize) {
		this.fileSize = fileSize;
	}

	public String getBeginTime() {
		return beginTime;
	}

	public void setBeginTime(String beginTime) {
		this.beginTime = beginTime;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
	
	
	
}
