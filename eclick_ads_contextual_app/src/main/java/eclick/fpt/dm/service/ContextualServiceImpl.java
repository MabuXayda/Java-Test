package eclick.fpt.dm.service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.fpt.dm.EclickConstant;
import com.fpt.dm.configs.RedisPoolConfigs;
import com.fpt.fo.contextual.UrlContextual;
import com.google.gson.Gson;

import eclick.fpt.dm.redis.DataMiningRedisDao;
import eclick.fpt.dm.subscriber.MessageSubscriber;
import eclick.fpt.dm.subscriber.MessageSubscriberFactory;
import eclick.fpt.dm.subscriber.MessageSubscriberQueue;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class ContextualServiceImpl {

	static Logger LOGGER = Logger.getLogger(ContextualServiceImpl.class);

	static {
		PropertyConfigurator.configure("./conf/log4j.properties");
	}

	private int numOfThread;
	private Semaphore m_semaphoreForProcessUrl;
	private UrlContextual urlContextual;
	static RedisPoolConfigs redisConfigs = RedisPoolConfigs.load();

	private long startTime = System.currentTimeMillis();
	private AtomicInteger repeatEclick = new AtomicInteger(0);
	private AtomicInteger processedEclick = new AtomicInteger(0);
	private AtomicInteger noContentEclick = new AtomicInteger(0);
	private AtomicInteger repeatBanner = new AtomicInteger(0);
	private AtomicInteger processedBanner = new AtomicInteger(0);
	private AtomicInteger noContentBanner = new AtomicInteger(0);

	public static void main(String[] args) throws Exception {
		ContextualServiceImpl contextualServiceImpl = new ContextualServiceImpl(Integer.parseInt(args[0]));
		contextualServiceImpl.init();
	}

	public ContextualServiceImpl(int numThread) {
		numOfThread = numThread;
		m_semaphoreForProcessUrl = new Semaphore(numOfThread);
		urlContextual = new UrlContextual();
	}

	public void init() throws Exception {
		MessageSubscriberQueue messageSubscriberQueue = new MessageSubscriberQueue();
		startMessagerListener(messageSubscriberQueue);
		processMessageUrls(messageSubscriberQueue);
	}

	private void processMessageUrls(MessageSubscriberQueue messageSubscriberQueue) {
		Queue<Map<String, Object>> queueEclick = new ConcurrentLinkedQueue<>();
		Queue<Map<String, Object>> queueBanner = new ConcurrentLinkedQueue<>();
		new Thread(new Runnable() {
			@Override
			public void run() {
				Map<String, Object> object = null;
				while (true) {
					try {
						object = messageSubscriberQueue.getEclickObjectQueue().poll();
					} catch (Exception e) {
						// TODO: handle exception
					}
					if (object != null) {
						if (object.containsKey("banner_id")) {
							queueBanner.add(object);
						} else {
							queueEclick.add(object);
						}
					}
				}
			}
		}).start();
		new Thread(new Runnable() {

			@Override
			public void run() {
				while (true) {
					try {
						if (queueEclick.size() > 2000000) {
							queueEclick.clear();
						}
						System.out.println("ECLICK QUEUE: " + queueEclick.size() + " - repeat: " + repeatEclick
								+ " - processed: " + processedEclick + " - error: " + noContentEclick
								+ " | BANNER QUEUE: " + queueBanner.size() + " - repeat: " + repeatBanner
								+ " - processed: " + processedBanner + " - error: " + noContentBanner);
						long runTime = System.currentTimeMillis();
						if ((runTime - startTime) > (24 * 60 * 60 * 1000)) {
							queueEclick.clear();
							queueBanner.clear();
							LOGGER.info("ECLICK QUEUE: " + queueEclick.size() + " - repeat: " + repeatEclick
									+ " - processed: " + processedEclick + " - error: " + noContentEclick
									+ " | BANNER QUEUE: " + queueBanner.size() + " - repeat: " + repeatBanner
									+ " - processed: " + processedBanner + " - error: " + noContentBanner);
							startTime = runTime;
							repeatEclick.set(0);
							repeatBanner.set(0);
							processedEclick.set(0);
							processedBanner.set(0);
							noContentEclick.set(0);
							noContentBanner.set(0);
						}
						Thread.sleep(1000 * 30);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}).start();
		processMessageEclick(queueEclick);
		processMessageBanner(queueBanner);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void processMessageEclick(Queue<Map<String, Object>> queueEclick) {
		Thread thread = new Thread(() -> {
			while (true) {
				try {
					final Map<String, Object> object = queueEclick.poll();
					if (object != null) {
						Thread subthread = new Thread(() -> {
							String url = object.get("full_url").toString();
							String objectId = object.get("object_id").toString();
							try {
								m_semaphoreForProcessUrl.acquire();
								if (!isExistKeyFromRedisArticleTopics("ect:" + objectId)) {
									Long crawTime1 = System.currentTimeMillis();
									String urlContent = urlContextual.getUrlContent(url);
									Long crawTime2 = System.currentTimeMillis();
									if (urlContent != null && !urlContent.isEmpty()) {
										Long processTime1 = System.currentTimeMillis();
										Map keywords = urlContextual.getUrlTopKeyword(urlContent, 20);
										Map topics = urlContextual.getUrlTopTopic(urlContent, 20);
										Map result = new HashMap<>();
										result.put("obj_id", objectId);
										result.put("topic", topics);
										result.put("keyword", keywords);
										String data = new Gson().toJson(result);
										// saveArticleContent(objectId,
										// urlContent, 7);
										saveArticleTopicsAndConcepts(objectId, data, 7);
										DataMiningRedisDao.publishArticleTopicsAndConcepts(
												EclickConstant.KEY_PUB_SUB_ECLICK2, data);
										Long processTime2 = System.currentTimeMillis();
										LOGGER.info("===> DONE PROCESS URL: " + url + " | " + objectId + " | CRAWL: "
												+ (crawTime2 - crawTime1) + " | PROCESS: "
												+ (processTime2 - processTime1));
										processedEclick.incrementAndGet();
									} else {
										Map result = Collections.emptyMap();
										String data = new Gson().toJson(result);
										saveArticleTopicsAndConcepts(objectId, data, 7);
										LOGGER.info("===> DONE TEMPORARY URL: " + url + " | " + objectId + " | CRAWL: "
												+ (crawTime2 - crawTime1));
										noContentEclick.incrementAndGet();
									}
								} else {
									repeatEclick.incrementAndGet();
								}
							} catch (Exception e) {
								LOGGER.error(e.getMessage());
							} finally {
								m_semaphoreForProcessUrl.release();
							}
						});
						subthread.start();
					}
				} catch (Exception e) {
					e.fillInStackTrace();
					LOGGER.error(ExceptionUtils.getStackTrace(e));
				}
			}
		});
		thread.start();
	}

	private void processMessageBanner(Queue<Map<String, Object>> queueBanner) {
		new Thread(new Runnable() {

			@SuppressWarnings({ "rawtypes", "unchecked" })
			@Override
			public void run() {
				while (true) {
					try {
						Map<String, Object> object = queueBanner.poll();
						if (object != null) {
							String url = object.get("full_url").toString();
							String objectId = object.get("object_id").toString();
							String bannerId = object.get("banner_id").toString();
							if (!isExistKeyFromRedisArticleTopics("ect:" + objectId)) {
								String urlContent = urlContextual.getUrlContent(url);
								if (urlContent != null && !urlContent.isEmpty()) {
									Map keywords = urlContextual.getUrlTopKeyword(urlContent, 20);
									Map topics = urlContextual.getUrlTopTopic(urlContent, 20);
									Map result = new HashMap<>();
									result.put("obj_id", objectId);
									result.put("topic", topics);
									result.put("keyword", keywords);
									String data = new Gson().toJson(result);
									// saveArticleContent(objectId, urlContent,
									// 7);
									saveArticleTopicsAndConcepts(objectId, data, 7);
									result.put("banner_id", bannerId);
									DataMiningRedisDao
											.publishArticleTopicsAndConcepts(EclickConstant.KEY_PUB_SUB_ECLICK2, data);
									LOGGER.info("===> DONE PROCESS URL: " + url + " | " + objectId);
									processedBanner.incrementAndGet();
								} else {
									Map result = Collections.emptyMap();
									String data = new Gson().toJson(result);
									saveArticleTopicsAndConcepts(objectId, data, 7);
									LOGGER.info("===> DONE TEMPORARY URL: " + url + " | " + objectId);
									noContentBanner.incrementAndGet();
								}
							} else {
								repeatBanner.incrementAndGet();
							}
						}
					} catch (Exception e) {
						e.fillInStackTrace();
						LOGGER.error(ExceptionUtils.getStackTrace(e));
					}
				}
			}
		}).start();
	}

	private void startMessagerListener(MessageSubscriberQueue messageSubscriberQueue) {
		startMessageListener("ECLICK", messageSubscriberQueue);
	}

	private void startMessageListener(String messageSubscriberType, MessageSubscriberQueue messageSubscriberQueue) {
		String host = "";
		int port = 0;
		if (messageSubscriberType.equalsIgnoreCase("ECLICK")) {
			host = redisConfigs.getRedisPubSubEclick().get("host");
			port = Integer.parseInt(redisConfigs.getRedisPubSubEclick().get("port"));
		} else if (messageSubscriberType.equalsIgnoreCase("SOHOA")) {
			host = redisConfigs.getRedisPubSubSoHoa().get("host");
			port = Integer.parseInt(redisConfigs.getRedisPubSubSoHoa().get("port"));
		}
		System.out.println(messageSubscriberType);
		System.out.println("host: " + host);
		System.out.println("port: " + port);
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		JedisPool jedisPool = new JedisPool(poolConfig, host, port, 0);
		Jedis subscriberJedis = jedisPool.getResource();
		MessageSubscriberFactory messageSubscriberFactory = new MessageSubscriberFactory();
		MessageSubscriber messageSubscriber = messageSubscriberFactory.getMessageSubscriber(messageSubscriberType,
				messageSubscriberQueue);
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					if (messageSubscriberType.equalsIgnoreCase("ECLICK")) {
						subscriberJedis.subscribe(messageSubscriber, EclickConstant.KEY_PUB_SUB_ECLICK1);

					} else if (messageSubscriberType.equalsIgnoreCase("SOHOA")) {
						subscriberJedis.subscribe(messageSubscriber, EclickConstant.KEY_PUB_SUB_SO_HOA);
					}
				} catch (Exception e) {
					LOGGER.error("Subscribing failed.", e);
				}
			}
		}).start();
	}

	private static void saveArticleContent(String objectId, String data, Integer expireDay) {
		DataMiningRedisDao.saveArticleContent(objectId, data, expireDay);
	}

	private static void saveArticleTopicsAndConcepts(String objectId, String data, Integer expireDay) {
		DataMiningRedisDao.saveArticleTopicsAndConcepts(objectId, data, expireDay);
	}

	private static boolean isExistKeyFromRedisArticleTopics(String key) {
		return DataMiningRedisDao.isExistKeyFromRedisArticleTopics(key);
	}

}