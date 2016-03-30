package com.fpt.fo.kafka;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.message.Message;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import scala.collection.Seq;

public class SimpleKafka {
	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.put("metadata.broker.list", "localhost:9092");
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("partitioner.class", "example.producer.SimplePartitioner");
		properties.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(properties);

		Producer<String, String> producer = new Producer<>(config);

		Random random = new Random();

		for (int i = 0; i < 20; i++) {
			long runtime = new Date().getTime();
			String ip = "192.168.2." + random.nextInt(255);
			String msg = runtime + ",www.example.com," + ip;

			KeyedMessage<String, String> data = new KeyedMessage<String, String>("test", ip, msg);
			producer.send((Seq<KeyedMessage<String, String>>) data);
		}
		producer.close();
	}
}
