package com.xperia.xpense_consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {
		"com.xperia.xpense_consumer",
		"org.xperia.repository",
		"org.xperia.service"
})
public class XpenseConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(XpenseConsumerApplication.class, args);
	}

}
