package com.mikerusoft.playground.generatedata;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class GeneratorApp implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(GeneratorUdhiApp.class, args);
    }

    @Autowired
    private GeneratorUdhiApp udhiApp;

    @Autowired
    private MonitorMessagingGenerator monitorApp;

    @Override
    public void run(String... args) throws Exception {
        // udhiApp.run();
        monitorApp.run();
    }
}
