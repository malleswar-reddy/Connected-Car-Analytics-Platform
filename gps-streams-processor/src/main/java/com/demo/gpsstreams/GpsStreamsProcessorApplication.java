package com.demo.gpsstreams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class GpsStreamsProcessorApplication {
    public static void main(String[] args) {
        SpringApplication.run(GpsStreamsProcessorApplication.class, args);
    }
}
