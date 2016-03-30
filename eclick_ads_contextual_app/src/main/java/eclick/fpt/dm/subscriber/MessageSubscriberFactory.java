package eclick.fpt.dm.subscriber;

public class MessageSubscriberFactory {
	// use getMessageSubscriber method to get object of type shape
	public MessageSubscriber getMessageSubscriber(String messageSubscriberType,
			MessageSubscriberQueue messageSubscriberQueue) {
		if (messageSubscriberType == null) {
			return null;
		}
		if (messageSubscriberType.equalsIgnoreCase("ECLICK")) {
			return new MessageSubscriberEclick(messageSubscriberQueue);

		} else if (messageSubscriberType.equalsIgnoreCase("SOHOA")) {
			return new MessageSubscriberSoHoa(messageSubscriberQueue);
		}

		return null;
	}
}
