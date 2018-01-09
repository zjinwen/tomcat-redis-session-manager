package com.orangefunction.tomcat.redissessions;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.resource.ClientResources;

public class ClusterNode {
	public String name;
	public RedisClusterClient clusterClient;
	public ClientResources res;
	public List<StatefulRedisClusterConnection<byte[], byte[]>> connections;
	private int connectionSize;
	private List<StatefulRedisClusterConnection<String, String>> stringConnections;
	private int stringConnectionSize;
	public ClusterNode(String name,
			RedisClusterClient clusterClient,
	        ClientResources res,
			List<StatefulRedisClusterConnection<byte[], byte[]>> connections,List<StatefulRedisClusterConnection<String,String>> stringConnections) {
		super();
		this.name = name;
		this.clusterClient = clusterClient;
		this.res = res;
		
		this.connections = connections;
		this.connectionSize=connections.size();
		this.stringConnections = stringConnections;
		this.stringConnectionSize=stringConnections.size();
	}
	public  RedisAdvancedClusterCommands<String, String> getStringCommand() {
		if(this.stringConnectionSize==0) {
			throw new IllegalArgumentException("string connection is empty");
		}
		int sel=ThreadLocalRandom.current().nextInt(this.stringConnectionSize);
		RedisAdvancedClusterCommands<String, String> cmd = stringConnections.get(sel).sync();
		return cmd;
	}
	public RedisAdvancedClusterCommands<byte[], byte[]> getConnection() {
		if(this.connectionSize==0) {
			throw new IllegalArgumentException("byte[] connection is empty");
		}
		int sel=ThreadLocalRandom.current().nextInt(this.connectionSize);
		RedisAdvancedClusterCommands<byte[], byte[]> cmd = connections.get(sel).sync();
		return cmd;
	}
	public void close() {
		for(StatefulRedisClusterConnection<byte[], byte[]> connection:connections) {
			connection.close();
		}
		for(StatefulRedisClusterConnection<String, String> connection:stringConnections) {
			connection.close();
		}
		
		if(clusterClient!=null) {
			clusterClient.shutdown();
		}
		if(res!=null) {
			res.shutdown();
		}
		
	}
}
