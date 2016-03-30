package eclick.fpt.dm.subscriber;

import java.util.Map;

import com.google.gson.Gson;

public class MessageSubscriberEclick extends MessageSubscriber {

	public MessageSubscriberEclick() {
		super();
	}

	public MessageSubscriberEclick(MessageSubscriberQueue messageSubscriberQueue) {
		super();
		this.setMessageSubscriberQueue(messageSubscriberQueue);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onMessage(String channel, String message) {
		Map<String, Object> messageObject = new Gson().fromJson(message, Map.class);
		if (!this.getMessageSubscriberQueue().getEclickObjectQueue().contains(messageObject)) {
			this.getMessageSubscriberQueue().addEclickObject(messageObject);
		}
	}
}
