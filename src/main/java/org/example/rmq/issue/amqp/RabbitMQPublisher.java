package org.example.rmq.issue.amqp;


import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import io.quarkus.runtime.StartupEvent;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;


@Slf4j
@ApplicationScoped
public class RabbitMQPublisher {

	@Inject
	RabbitMQClient rabbitMQClient;

	Channel channel;

	public void onApplicationStart(@Observes StartupEvent event) {
		System.out.println("RabbitMqExchangePublisher.onApplicationStart");
		// on application start prepare the queues and message listener
		setupQueues();
//		setupReceiving();
	}

	private void setupQueues() {
		try {
			System.out.println("RabbitMqExchangePublisher.setupQueues");
			// create a connection
			Connection connection = rabbitMQClient.connect();
			// create a channel
			channel = connection.createChannel();
			// declare exchanges and queues
			channel.exchangeDeclare("issue-exchange", BuiltinExchangeType.FANOUT, true);
			channel.queueDeclare("issue-queue", true, false, false, null);
			channel.queueBind("issue-queue", "issue-exchange", "");
		} catch (IOException e) {
			log.error("", e);
			throw new UncheckedIOException(e);
		}
	}

	public void send(String message) {
		try {
			// send a message to the exchange
			channel.basicPublish("issue-exchange", "", null, message.getBytes(StandardCharsets.UTF_8));
		} catch (IOException e) {
			log.error("", e);
			throw new UncheckedIOException(e);
		}
	}

}
