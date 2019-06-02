package com.mikerusoft.playground.generatedata;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class GeneratorApp implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(GeneratorApp.class, args);
    }

    @Autowired
    private ApplicationContext context;

    @Value("${app.name:counter_ex_window}") // "udhi", "monitor"
    private String appName;

    @Override
    public void run(String... args) throws Exception {
        Generator generator = context.getBean(appName, Generator.class);
        generator.run();
    }
}
