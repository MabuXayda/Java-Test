package eclick.fpt.dm.redis;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisException;

import com.fpt.dm.configs.RedisPoolConfigs;
import com.fpt.fo.core.utils.StringPool;

public class DataMiningRedisDao {

	static ShardedJedisPool redisArticleTopics = null;
	static ShardedJedisPool redisPubSubEclick = null;

	public static ShardedJedisPool getRedisArticleTopicsPool(String host, int port) {
		if (redisArticleTopics == null) {
			RedisPoolConfigs redisConfigs = RedisPoolConfigs.load();
			List<JedisShardInfo> redisArticleTopicsShard = new ArrayList<JedisShardInfo>();
			redisArticleTopicsShard.add(new JedisShardInfo(host, port, 0));
			redisArticleTopics = new ShardedJedisPool(redisConfigs.getJedisPoolConfig(), redisArticleTopicsShard);
		}
		return redisArticleTopics;
	}

	public static ShardedJedisPool getRedisPubSubEclickPool(String host, int port) {
		if (redisPubSubEclick == null) {
			RedisPoolConfigs redisConfigs = RedisPoolConfigs.load();
			List<JedisShardInfo> redisArticleTopicsShard = new ArrayList<JedisShardInfo>();
			redisArticleTopicsShard.add(new JedisShardInfo(host, port, 0));
			redisPubSubEclick = new ShardedJedisPool(redisConfigs.getJedisPoolConfig(), redisArticleTopicsShard);
		}
		return redisPubSubEclick;
	}

	public static ShardedJedisPool getRedisArticleTopicsPool() {
		RedisPoolConfigs redisConfigs = RedisPoolConfigs.load();
		redisConfigs.setMaxActive(22);
		String host = redisConfigs.getRedisArticleTopics().get("host");
		int port = Integer.parseInt(redisConfigs.getRedisArticleTopics().get("port"));
		return getRedisArticleTopicsPool(host, port);
	}
	
	public static ShardedJedisPool getRedisArticleContentPool() {
		RedisPoolConfigs redisConfigs = RedisPoolConfigs.load();
		redisConfigs.setMaxActive(22);
		String host = redisConfigs.getRedisArticleContent().get("host");
		int port = Integer.parseInt(redisConfigs.getRedisArticleContent().get("port"));
		return getRedisArticleTopicsPool(host, port);
	}

	public static ShardedJedisPool getRedisPubSubEclickPool() {
		RedisPoolConfigs redisConfigs = RedisPoolConfigs.load();
		String host = redisConfigs.getRedisPubSubEclick().get("host");
		int port = Integer.parseInt(redisConfigs.getRedisPubSubEclick().get("port"));
		return getRedisPubSubEclickPool(host, port);
	}

	public static void saveArticleContent(String objectId, String data, Integer expireDay) {
		ShardedJedisPool jedisPool = DataMiningRedisDao.getRedisArticleContentPool();
		ShardedJedis shardedJedis = null;
		boolean isCommited = false;
		try {
			shardedJedis = jedisPool.getResource();
			Jedis jedis = shardedJedis.getShard(StringPool.BLANK);

			// jedis.set("ect:" + objectId, data);
			Integer expireTime = 86400 * expireDay;
			jedis.setex("ect:" + objectId, expireTime, data);
			jedis.close();
			isCommited = true;
		} catch (JedisException e) {

		} finally {
			if (shardedJedis != null) {
				if (isCommited) {
					jedisPool.returnResource(shardedJedis);
				} else {
					jedisPool.returnBrokenResource(shardedJedis);
				}
			}
		}
	}

	public static void saveArticleTopicsAndConcepts(String objectId, String data, Integer expireDay) {
		ShardedJedisPool jedisPool = DataMiningRedisDao.getRedisArticleTopicsPool();
		ShardedJedis shardedJedis = null;
		boolean isCommited = false;
		try {
			shardedJedis = jedisPool.getResource();
			Jedis jedis = shardedJedis.getShard(StringPool.BLANK);

			// jedis.set("ect:" + objectId, data);
			Integer expireTime = 86400 * expireDay;
			jedis.setex("ect:" + objectId, expireTime, data);
			jedis.close();
			isCommited = true;
		} catch (JedisException e) {

		} finally {
			if (shardedJedis != null) {
				if (isCommited) {
					jedisPool.returnResource(shardedJedis);
				} else {
					jedisPool.returnBrokenResource(shardedJedis);
				}
			}
		}
	}

	public static boolean isExistKeyFromRedisArticleTopics(String key) {
		Boolean exit = false;
		ShardedJedisPool jedisPool = DataMiningRedisDao.getRedisArticleTopicsPool();
		ShardedJedis shardedJedis = null;
		boolean isCommited = false;
		try {
			shardedJedis = jedisPool.getResource();
			Jedis jedis = shardedJedis.getShard(StringPool.BLANK);
			exit = jedis.exists(key);
			jedis.close();
			isCommited = true;
		} catch (JedisException e) {

		} finally {
			if (shardedJedis != null) {
				if (isCommited) {
					jedisPool.returnResource(shardedJedis);
				} else {
					jedisPool.returnBrokenResource(shardedJedis);
				}
			}
		}
		return exit;
	}

	public static void publishArticleTopicsAndConcepts(String channel, String objectId) {
		ShardedJedisPool jedisPool = DataMiningRedisDao.getRedisPubSubEclickPool();
		ShardedJedis shardedJedis = null;
		boolean isCommited = false;
		try {
			shardedJedis = jedisPool.getResource();
			Jedis jedis = shardedJedis.getShard(StringPool.BLANK);
			jedis.publish(channel, objectId);
			isCommited = true;
		} catch (JedisException e) {

		} finally {
			if (shardedJedis != null) {
				if (isCommited) {
					jedisPool.returnResource(shardedJedis);
				} else {
					jedisPool.returnBrokenResource(shardedJedis);
				}
			}
		}
	}
}
