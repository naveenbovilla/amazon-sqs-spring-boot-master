package com.bfa.sqs.listener;

import java.util.List;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.bfa.sqs.service.RankService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

@Component
public class NotificationListener{
	
	private static final Logger log = LoggerFactory.getLogger(NotificationListener.class);
	
//	@Value("${rest.endpoint.notification}")
//	private String notificationUrl;
	
	@Value("${sqs.url}")
	private String sqsURL;

	@Autowired
	RankService rs;

	@Scheduled(fixedRate = 1000)
	public void getMessage() {
		BasicAWSCredentials credentials = new BasicAWSCredentials("XXXXXXXXXXXXXX", "****************************");
		final AmazonSQS sqs = AmazonSQSClientBuilder.standard()
				.withRegion("us-east-2")
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.build();
        while(true) {
        	log.info("Receiving messages from MyQueue.\n");
        	final ReceiveMessageRequest receiveMessageRequest =
                    new ReceiveMessageRequest(sqsURL)
                    	.withMaxNumberOfMessages(1)
                    	.withWaitTimeSeconds(3);
	        final List<com.amazonaws.services.sqs.model.Message> messages = sqs.receiveMessage(receiveMessageRequest)
	                .getMessages();
	        for (final com.amazonaws.services.sqs.model.Message message : messages) {
	        	log.debug("Message");
	        	log.debug("  MessageId:     "
	                    + message.getMessageId());
	        	log.debug("  ReceiptHandle: "
	                    + message.getReceiptHandle());
	        	log.debug("  MD5OfBody:     "
	                    + message.getMD5OfBody());
	        	log.debug("  Body:          "
	                    + message.getBody());
	            if(!"".equals(message.getBody())) {
	            	log.info("Calling rank service to insert records into database");
//		            RestTemplate rest = new RestTemplate();
//		            ResponseEntity<String> companyInfoResponse = rest.postForEntity(notificationUrl, message.getBody(), String.class);
//		            String s = companyInfoResponse.getBody();
//
//		            System.out.println("Deleting a message.\n");
//		            final String messageReceiptHandle = messages.get(0).getReceiptHandle();
//		            sqs.deleteMessage(new DeleteMessageRequest(sqsURL,
//		                    messageReceiptHandle));

					rs.rankAndInsert();
					sqs.deleteMessage(sqsURL, message.getReceiptHandle());


		         }
	        }
        }
	}
}
