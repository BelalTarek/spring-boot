package com.gauravg.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import com.gauravg.model.Model;

import java.util.Map;


@Component
public class ReplyingKafkaConsumer<consumerRecord> {
	@Autowired
	ReplyingKafkaTemplate<String, Model,Model> kafkaTemplate;
	@Value("${kafka.topic.requestreply-topic}")
	String requestReplyTopic;
	 @KafkaListener(topics = "${kafka.topic.request-topic}")


//	 @SendTo
	  public void listen(Model request, @Header(KafkaHeaders.CORRELATION_ID) byte[] correlationidbytes , @Header("__TypeId__") byte[] typeId) throws InterruptedException {

		 int sum = request.getFirstNumber() + request.getSecondNumber();
		 request.setAdditionalProperty("sum", sum);
		 String correlationId = new String (correlationidbytes);

		 Map<String , Object> map = request.getAdditionalProperties();

				 Message<Model> message = MessageBuilder
				 .withPayload(request)
				 .setHeader(KafkaHeaders.TOPIC, requestReplyTopic)
				 .setHeader(KafkaHeaders.CORRELATION_ID, correlationidbytes)
				 .setHeader(("__TypeId__"), typeId)
				 .build();
//
		 kafkaTemplate.send(message);
//		 return consumerRecord;


}
}
