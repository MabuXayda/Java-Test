package eclick.fpt.dm.subscriber;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MessageSubscriberQueue {
	private Queue<Map<String, Object>> eclickObjectQueue = new ConcurrentLinkedQueue<Map<String, Object>>();
	private Queue<Map<String, Object>> sohoaObjectQueue = new ConcurrentLinkedQueue<Map<String, Object>>();

	public Queue<Map<String, Object>> getEclickObjectQueue() {
		return eclickObjectQueue;
	}

	public void setEclickObjectQueue(Queue<Map<String, Object>> eclickObjectQueue) {
		this.eclickObjectQueue = eclickObjectQueue;
	}

	public Queue<Map<String, Object>> getSohoaObjectQueue() {
		return sohoaObjectQueue;
	}

	public void setSohoaObjectQueue(Queue<Map<String, Object>> sohoaObjectQueue) {
		this.sohoaObjectQueue = sohoaObjectQueue;
	}

	public void addEclickObject(Map<String, Object> item) {
		eclickObjectQueue.add(item);
	}

	public void addSohoaObject(Map<String, Object> item) {
		sohoaObjectQueue.add(item);
	}
}
