package com.bfa.sqs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Controller;

@SpringBootApplication
@EnableScheduling
public class BfaAwsQueueApplication {
	
	public static void main(String[] args) {
		SpringApplication.run(BfaAwsQueueApplication.class, args);
	}
}
