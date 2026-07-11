package com.demo.gpsstreams.config;

import com.demo.gpsstreams.websocket.CarPositionWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final CarPositionWebSocketHandler carPositionWebSocketHandler;

    public WebSocketConfig(CarPositionWebSocketHandler carPositionWebSocketHandler) {
        this.carPositionWebSocketHandler = carPositionWebSocketHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(carPositionWebSocketHandler, "/ws/car-positions")
                // demo-only: wide open CORS for the WebSocket handshake so any
                // frontend origin can connect. Lock this down to your real
                // frontend origin(s) before this goes anywhere near production.
                .setAllowedOrigins("*");
    }
}
