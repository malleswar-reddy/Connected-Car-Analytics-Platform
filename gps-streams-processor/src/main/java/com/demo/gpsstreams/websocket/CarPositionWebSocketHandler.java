package com.demo.gpsstreams.websocket;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * One-way broadcast channel: frontend clients connect and just listen, they
 * don't send anything meaningful back. Deliberately raw WebSocket (no STOMP)
 * since there's only one topic and no need for pub/sub routing.
 */
@Slf4j
@Component
public class CarPositionWebSocketHandler extends TextWebSocketHandler {

    private final Set<WebSocketSession> sessions = new CopyOnWriteArraySet<>();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        sessions.add(session);
        log.info("Frontend client connected: {} ({} total)", session.getId(), sessions.size());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session);
        log.info("Frontend client disconnected: {} ({} total)", session.getId(), sessions.size());
    }

    public void broadcast(String json) {
        TextMessage message = new TextMessage(json);
        for (WebSocketSession session : sessions) {
            try {
                if (session.isOpen()) {
                    session.sendMessage(message);
                } else {
                    sessions.remove(session);
                }
            } catch (IOException e) {
                log.warn("Failed to send to session {}, dropping it", session.getId(), e);
                sessions.remove(session);
            }
        }
    }

    public int connectedClientCount() {
        return sessions.size();
    }
}
