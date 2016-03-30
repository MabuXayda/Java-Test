package eclick.fpt.dm.subscriber;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

public class MessageSubscriberSoHoa extends MessageSubscriber {

	public MessageSubscriberSoHoa() {
		super();
	}

	public MessageSubscriberSoHoa(MessageSubscriberQueue messageSubscriberQueue) {
		super();
		this.setMessageSubscriberQueue(messageSubscriberQueue);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onMessage(String channel, String message) {
		Map<String, Object> messageObject = new Gson().fromJson(message, HashMap.class);
		if (!this.getMessageSubscriberQueue().getSohoaObjectQueue().contains(messageObject)) {
			System.out.println("save mess to queue: " + channel);
			this.getMessageSubscriberQueue().addSohoaObject(messageObject);
			System.out.println("check size: " + this.getMessageSubscriberQueue().getSohoaObjectQueue().size());
		}
	}
}
