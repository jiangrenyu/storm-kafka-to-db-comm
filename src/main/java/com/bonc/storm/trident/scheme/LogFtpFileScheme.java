package com.bonc.storm.trident.scheme;

import com.bonc.storm.bean.Message;
import net.sf.json.JSONObject;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

public class LogFtpFileScheme implements Scheme{

	private static final Logger LOG = LoggerFactory.getLogger(LogFtpFileScheme.class);

    public static final String MESSAGE_SCHEME_KEY = "source_msg";

	public List<Object> deserialize(ByteBuffer ser) {
		CharBuffer charBuffer = null;
		try {
			Charset charset = Charset.forName("UTF-8");
			CharsetDecoder decoder = charset.newDecoder();
			charBuffer = decoder.decode(ser);
			ser.flip();
			return new Values(deserializeJson(charBuffer.toString()));
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}

	/**
	 * 将topic中的消息反序列化为json对象
	 * @param ser
	 * @return
	 */
	public Message deserializeJson(String ser){
		 try {
			 LOG.info("receive message："+ ser );
			 JSONObject jsonObj = JSONObject.fromObject(ser);
			 
			 Message msg = new Message();
			 msg.setFileName(jsonObj.getString("file_name"));
			 msg.setOperateType(jsonObj.getInt("operate_type"));
			 msg.setClientAddress(jsonObj.getString("client_address"));
			 msg.setServerAddress(jsonObj.getString("server_address"));
			 msg.setFtpAccount(jsonObj.getString("ftp_account"));
			 msg.setFilePath(jsonObj.getString("file_path"));
			 msg.setVersion(jsonObj.getString("version"));
			 msg.setFileSize(jsonObj.getDouble("file_size"));
			 msg.setBeginTime(jsonObj.getString("begin_time"));
			 msg.setEndTime(jsonObj.getString("end_time"));
			 
			 return msg;
		} catch (Exception e) {
			LOG.info("转换 JSON 消息异常：",e);
		}
		 return null;
	}
	
	@Override
	public Fields getOutputFields() {
        return new Fields(MESSAGE_SCHEME_KEY);
	}

}
