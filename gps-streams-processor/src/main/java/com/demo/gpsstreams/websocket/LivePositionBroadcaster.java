package com.demo.gpsstreams.websocket;

import com.demo.gpsstreams.model.CarPosition;
import com.demo.gpsstreams.repository.LivePositionRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class LivePositionBroadcaster {

    private final LivePositionRepository repository;
    private final CarPositionWebSocketHandler webSocketHandler;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public LivePositionBroadcaster(LivePositionRepository repository,
                                   CarPositionWebSocketHandler webSocketHandler) {
        this.repository = repository;
        this.webSocketHandler = webSocketHandler;
    }

    @Scheduled(fixedRateString = "${gps.live-tracking-interval-ms:1000}")
    public void pushLatestPositions() {
        // Skip the DB round-trip entirely if nobody's listening.
        if (webSocketHandler.connectedClientCount() == 0) {
            return;
        }
        try {
            List<CarPosition> positions = repository.findLatestPerCar();
            System.out.println("Broadcasting " + positions.size() + " live positions to " + webSocketHandler.connectedClientCount() + " clients");
            String json = objectMapper.writeValueAsString(positions);
            webSocketHandler.broadcast(json);
        } catch (Exception e) {
            log.error("Failed to push live positions", e);
        }
    }
}
