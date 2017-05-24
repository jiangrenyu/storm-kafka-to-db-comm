package com.bonc.storm.trident;

import com.bonc.storm.bean.PropertyConfig;
import com.bonc.storm.config.FieldMapperConfiguration;
import com.bonc.storm.jdbc.C3P0ConnectionProvider;
import com.bonc.storm.jdbc.ConnectionProvider;
import com.bonc.storm.trident.state.JdbcStateFactory;
import com.bonc.storm.trident.state.JdbcUpdater;
import com.bonc.storm.util.FileUtil;
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
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaToDbTopology {
	
	private static String topologyName = null;
	
	private static int spoutParallelism = 0;
	
	private static int mysqlBoltParallelism = 0;
	
	private static PropertyConfig config = null;

	private static Properties dbProperties = null;

	private static final Logger LOG = LoggerFactory.getLogger(KafkaToDbTopology.class);
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException{
		
		String usage = "Usage:KafkaToDbTopology [-topologyName <topologyName>] -spoutParallelism <spoutParallelism> -mysqlBoltParallelism <mysqlBoltParallelism> -configPath <configPath> -dbPath <dbPath>";
		
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
					System.err.println("configPath can't found");
					System.exit(-1);
				} 
			}else if("-dbPath".equals(args[i])){
				String dbPath = args[++i];
				try {
					dbProperties = FileUtil.getProperteis(dbPath);
					if(dbProperties == null){
						System.err.println("create dataSource error ,please check dbPath:"+dbPath);
						System.exit(-1);
					}
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
		
		//字段映射配置文件名
		String fieldMapperFile = config.getValue("fieldMapperFile");
		
		FieldMapperConfiguration configuration = null;
		try {
			configuration = FieldMapperConfiguration.getInstance(fieldMapperFile);
		} catch (Exception e2) {
			System.err.println("字段映射配置文件错误，请检查文件："+fieldMapperFile);
			System.exit(-1);
		}

		
		//批提交大小
		int batchSize = 400 ;
		
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
		
		BrokerHosts zk = new ZkHosts(zkHost);
		
		TridentKafkaConfig  spoutConf = null;
		
		if(spoutClientId == null || spoutClientId.startsWith("/")){
			System.err.println("参数配置错误，请检查配置文件中的 spoutClientId 参数，spoutClientId不能为空且不能以 / 开头");
			System.exit(-1);
		}

		spoutConf = new TridentKafkaConfig(zk, consumerTopic, spoutClientId);

		String forceFromStart = config.getValue("forceFromStart");
		if(forceFromStart != null){
			try {
				spoutConf.ignoreZkOffsets = Boolean.parseBoolean(forceFromStart);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		String startOffsetTime = config.getValue("startOffsetTime");
		if ("smallest".equals(startOffsetTime)) {
			spoutConf.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
		} else if ("largest".equals(startOffsetTime)) {
			spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		}
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

		TridentTopology topology = new TridentTopology();
		
		Stream stream = topology.newStream(spoutClientId, spout).parallelismHint(spoutParallelism);
		
		ConnectionProvider connectionProvider = new C3P0ConnectionProvider(dbProperties);
		
		JdbcStateFactory stateFactory = new JdbcStateFactory().withConnectionProvider(connectionProvider).withFieldMapperConfig(configuration).withBatchSize(batchSize);
		
		stream.partitionPersist(stateFactory, spout.getOutputFields(), new JdbcUpdater(), new Fields()).parallelismHint(mysqlBoltParallelism);
		
		Config conf = new Config();
		
//		conf.setNumWorkers(3);
		
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
