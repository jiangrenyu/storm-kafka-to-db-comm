package com.bonc.storm.trident;

import com.bonc.storm.bean.PropertyConfig;
import com.bonc.storm.jdbc.C3P0ConnectionProvider;
import com.bonc.storm.jdbc.ConnectionProvider;
import com.bonc.storm.jdbc.DBManager;
import com.bonc.storm.service.ProvService;
import com.bonc.storm.trident.mapper.LogFtpFileMapper;
import com.bonc.storm.trident.scheme.LogFtpFileScheme;
import com.bonc.storm.trident.state.LogFtpFileJdbcUpdater;
import com.bonc.storm.trident.state.LogFtpFileStateFactory;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class LogFtpFileTopology {
	
	private static String topologyName = null;
	
	private static int spoutParallelism = 0;
	
	private static int mysqlBoltParallelism = 0;
	
	private static PropertyConfig config = null;

	private static DBManager dbManager = null;
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException{
		
		String usage = "Usage:LogFtpFileTopology [-topologyName <topologyName>] -spoutParallelism <spoutParallelism> -mysqlBoltParallelism <mysqlBoltParallelism>  -configPath <configPath> -dbPath <dbPath>";
		
		if(args.length < 8){
			System.err.println("缺少参数");
			System.out.println(usage);
			System.exit(-1);
		}
		
		
		for(int i = 0 ; i < args.length ; i++){
			if("-topologyName".equals(args[i])){
				topologyName = args[++i];
			}else if("-spoutParallelism".equals(args[i])){
				try {
					spoutParallelism = Integer.parseInt(args[++i]);
				} catch (NumberFormatException e) {
					System.err.println("spoutParallelism must be a number");
					System.exit(-1);
				}
			}else if("-mysqlBoltParallelism".equals(args[i])){
				try {
					mysqlBoltParallelism = Integer.parseInt(args[++i]);
				} catch (NumberFormatException e) {
					System.err.println("mysqlBoltParallelism must be a number");
					System.exit(-1);
				}
			}else if("-configPath".equals(args[i])){
				String configPath = args[++i];
				try {
					config = new PropertyConfig(configPath);
				} catch (Exception e) {
					System.err.println("configPath can't found:"+configPath);
					System.exit(-1);
				} 
			}else if("-dbPath".equals(args[i])){
				String dbPath = args[++i];
				try {
					
					dbManager = new DBManager(dbPath);
				} catch (Exception e) {
					System.err.println("create dataSource error ,please check dbPath:"+dbPath);
					System.exit(-1);
				}
				
			}else{
				throw new IllegalArgumentException("arg " + args[i] + " not recognized");
			}
		}
		
		//kafka使用的zookeeper的host
		String zkHost = config.getValue("zkHost");
		
		//spout消费的topic
		String consumerTopic = config.getValue("consumerTopic");
		
		//spout信息保存在zookeeper的路径
		String spoutClientId = config.getValue("spoutClientId");
		
		//批提交大小
		int batchSize = 400;
		
		try {
			batchSize = Integer.parseInt(config.getValue("batchSize"));
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		
		if(zkHost == null || !checkHost(zkHost)){
			System.err.println("参数配置错误，请检查配置文件中的 zkHost 参数");
			System.exit(-1);
		}
		
		if(consumerTopic == null){
			System.err.println("参数配置错误，请检查配置文件中的 consumerTopic 参数");
			System.exit(-1);
		}
		
		ProvService provService = new ProvService(dbManager);
		
		Map<String, Integer> provMap = provService.getProvInfo();
		
		if(provMap == null || provMap.size() == 0){
			System.err.println("从数据库省份编码表d_prov中加载的内容为空");
			System.exit(-1);
		}
		
		BrokerHosts zk = new ZkHosts(zkHost);
		
		TridentKafkaConfig  spoutConf = null;
		
		if(spoutClientId == null || spoutClientId.startsWith("/")){
			System.err.println("参数配置错误，请检查配置文件中的 spoutClientId 参数，spoutClientId不能为空且不能以 / 开头");
			System.exit(-1);
		}
		
		spoutConf = new TridentKafkaConfig(zk, consumerTopic, spoutClientId);

		//使用LogFtpFileScheme 解析消息并定义输出字段
		spoutConf.scheme = new SchemeAsMultiScheme(new LogFtpFileScheme());
		
		String forceFromStart = config.getValue("forceFromStart");
		if(forceFromStart != null){
			try {
				spoutConf.ignoreZkOffsets = Boolean.parseBoolean(forceFromStart);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		//省份名称在文件绝对路径的位置
		int provNamePos = 2 ;
		
		try {
			provNamePos = Integer.parseInt(config.getValue("provNamePosition"));
		} catch (Exception e1) {
			e1.printStackTrace();
		} 
		
		//默认的省份编码
		int defaultProvId = -1;
		try {
			defaultProvId = Integer.parseInt(config.getValue("defaultProvId"));
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
		String startOffsetTime = config.getValue("startOffsetTime");
		if ("smallest".equals(startOffsetTime)) {
			spoutConf.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
		} else if ("largest".equals(startOffsetTime)) {
			spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		}
		TridentTopology topology = new TridentTopology();
		
		Stream stream = topology.newStream(spoutClientId, spout).parallelismHint(spoutParallelism);
		
		ConnectionProvider connectionProvider = new C3P0ConnectionProvider(dbManager.getProperties());
		
		String insertSql = "insert into  log_ftp_file(file_name ,operate_type,client_address,server_address,ftp_account,file_path,version,file_size,begin_time,end_time,prov_id,insert_time) values(?,?,?,?,?,?,?,?,?,?,?,?)";
		
		LogFtpFileMapper mapper = new LogFtpFileMapper(provMap,provNamePos,defaultProvId);
		
		LogFtpFileStateFactory stateFactory = new LogFtpFileStateFactory().withConnectionProvider(connectionProvider).withInsertStatement(insertSql).withJdbcMapper(mapper).withBatchSize(batchSize);
		
		stream.partitionPersist(stateFactory, spout.getOutputFields(), new LogFtpFileJdbcUpdater(), new Fields()).parallelismHint(mysqlBoltParallelism);
		
		Config conf = new Config();
		
		conf.setDebug(false);
		
		if(topologyName != null){
			
			StormSubmitter.submitTopology(topologyName, conf, topology.build());
			
		}else{
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, topology.build());
		}
		
		
		
	}
	
	
	/**
	 * 检查字符串是否符合 host1:ip,host2:ip 格式
	 * @param hostStr
	 * @return
	 */
	public static boolean checkHost(String hostStr){
		
		if(hostStr != null){
			String[] hosts = hostStr.split(",");
			if(hosts != null && hosts.length >0){
				for(String host : hosts){
					if(host.split(":") == null || host.split(":").length == 0){
						return false;
					}				
				}
				return true;
			}

		}

		return false;
	}
}
