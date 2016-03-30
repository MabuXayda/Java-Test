package com.fpt.dm.configs;

import java.io.Serializable;
import java.util.Map;

import com.fpt.dm.EclickConstant;
import com.fpt.fo.core.utils.FileUtils;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import redis.clients.jedis.JedisPoolConfig;

/**
 * Redis Pool Configs for all Storm Topology (in file
 * configs/redis-pool-configs.json)
 * 
 * @author trieu
 * 
 */
public class RedisPoolConfigs implements Serializable {

	private static final long serialVersionUID = -6047539971043372940L;

	static RedisPoolConfigs _instance;

	Map<String, String> redisArticleTopics;

	public Map<String, String> getRedisArticleTopics() {
		return redisArticleTopics;
	}

	public void setRedisArticleTopics(Map<String, String> redisArticleTopics) {
		this.redisArticleTopics = redisArticleTopics;
	}

	Map<String, String> redisArticleContent;
	
	public Map<String, String> getRedisArticleContent() {
		return redisArticleContent;
	}
	
	public void getRedisArticleContent(Map<String, String> redisArticleContent) {
		this.redisArticleContent = redisArticleContent;
	}
	
	Map<String, String> redisPubSubEclick;

	public Map<String, String> getRedisPubSubEclick() {
		return redisPubSubEclick;
	}

	public void setRedisPubSubEclick(Map<String, String> redisPubSubEclick) {
		this.redisPubSubEclick = redisPubSubEclick;
	}

	Map<String, String> redisPubSubSoHoa;

	public Map<String, String> getRedisPubSubSoHoa() {
		return redisPubSubSoHoa;
	}

	public void setRedisPubSubSoHoa(Map<String, String> redisPubSubSoHoa) {
		this.redisPubSubSoHoa = redisPubSubSoHoa;
	}

	// for the pool
	int maxActive = 20;

	int maxIdle = 10;

	int minIdle = 1;

	int maxWait = 3000;

	int numTestsPerEvictionRun = 10;

	boolean testOnBorrow = true;

	boolean testOnReturn = true;

	boolean testWhileIdle = true;

	int timeBetweenEvictionRunsMillis = 60000;

	public static final RedisPoolConfigs load() {
		if (_instance == null) {
			try {
				String json = FileUtils.readFileAsString(EclickConstant.REDIS_CONFIG_FILE);
				_instance = new Gson().fromJson(json, RedisPoolConfigs.class);
			} catch (Exception e) {
				if (e instanceof JsonSyntaxException) {
					e.printStackTrace();
					System.err.println("Wrong JSON syntax in file " + EclickConstant.REDIS_CONFIG_FILE);
				} else {
					e.printStackTrace();
				}
			}
		}
		return _instance;
	}

	public int getMaxActive() {
		return maxActive;
	}

	public void setMaxActive(int maxActive) {
		this.maxActive = maxActive;
	}

	public int getMaxIdle() {
		return maxIdle;
	}

	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	public int getMinIdle() {
		return minIdle;
	}

	public void setMinIdle(int minIdle) {
		this.minIdle = minIdle;
	}

	public int getMaxWait() {
		return maxWait;
	}

	public void setMaxWait(int maxWait) {
		this.maxWait = maxWait;
	}

	public int getNumTestsPerEvictionRun() {
		return numTestsPerEvictionRun;
	}

	public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
		this.numTestsPerEvictionRun = numTestsPerEvictionRun;
	}

	public boolean isTestOnBorrow() {
		return testOnBorrow;
	}

	public void setTestOnBorrow(boolean testOnBorrow) {
		this.testOnBorrow = testOnBorrow;
	}

	public boolean isTestOnReturn() {
		return testOnReturn;
	}

	public void setTestOnReturn(boolean testOnReturn) {
		this.testOnReturn = testOnReturn;
	}

	public boolean isTestWhileIdle() {
		return testWhileIdle;
	}

	public void setTestWhileIdle(boolean testWhileIdle) {
		this.testWhileIdle = testWhileIdle;
	}

	public int getTimeBetweenEvictionRunsMillis() {
		return timeBetweenEvictionRunsMillis;
	}

	public void setTimeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis) {
		this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
	}

	public String toJson() {
		return new Gson().toJson(this);
	}

	public JedisPoolConfig getJedisPoolConfig() {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(maxActive);
		config.setMaxIdle(maxIdle);
		config.setMinIdle(minIdle);
		config.setMaxWaitMillis(maxWait);
		config.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
		config.setTestOnBorrow(testOnBorrow);
		config.setTestOnReturn(testOnReturn);
		config.setTestWhileIdle(testWhileIdle);
		config.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
		return config;
	}

}
