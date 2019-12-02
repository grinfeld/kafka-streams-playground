package com.mikerusoft.playground.simple;

import com.mikerusoft.playground.simple.streams.Streamable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class SimpleApp implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication.run(SimpleApp.class, args);
    }

    @Value("${broker_url:localhost:9092}") private String url;
    @Value("${streamName:ktable-stream2}") private String streamName;
    @Autowired private ApplicationContext context;

    @Override
    public void run(String... args) throws Exception {
        context.getBean(streamName, Streamable.class).runStream(url);
    }
}
