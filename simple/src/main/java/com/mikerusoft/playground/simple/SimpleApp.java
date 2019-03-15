package com.mikerusoft.playground.simple;

import com.mikerusoft.playground.simple.streams.WindowStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SimpleApp implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication.run(SimpleApp.class, args);
    }

    @Value("${broker_url:localhost:9092}") private String url;

    @Override
    public void run(String... args) throws Exception {
        new WindowStream().runStream(url);
    }
}
