package com.demo.gpssimulator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class GpsSimulatorApplication {
    public static void main(String[] args) {
        SpringApplication.run(GpsSimulatorApplication.class, args);
    }
}
