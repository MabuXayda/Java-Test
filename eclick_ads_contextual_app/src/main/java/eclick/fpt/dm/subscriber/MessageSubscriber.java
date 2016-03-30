package eclick.fpt.dm.subscriber;

import redis.clients.jedis.JedisPubSub;

public abstract class MessageSubscriber extends JedisPubSub {
	private MessageSubscriberQueue messageSubscriberQueue;
	
	public MessageSubscriber() {
		super();
	}
	
	public MessageSubscriber(MessageSubscriberQueue messageSubscriberQueue) {
		super();
		this.messageSubscriberQueue = messageSubscriberQueue;
	}

	public MessageSubscriberQueue getMessageSubscriberQueue() {
		return messageSubscriberQueue;
	}

	public void setMessageSubscriberQueue(MessageSubscriberQueue messageSubscriberQueue) {
		this.messageSubscriberQueue = messageSubscriberQueue;
	}

	public void onMessage(String channel, String message) {
	}
}
