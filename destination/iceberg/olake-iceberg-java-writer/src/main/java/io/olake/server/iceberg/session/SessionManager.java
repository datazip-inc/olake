package io.olake.server.iceberg.session;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SessionManager {
    private final ConcurrentMap<String, WriterSession> sessions = new ConcurrentHashMap<>();

    public WriterSession getSession(String threadId) throws Exception {
        WriterSession session = sessions.get(threadId);
        if (session == null) {
            throw new Exception("No active session for thread " + threadId);
        }
        return session;
    }

    public WriterSession getOrCreateSession(String threadId, java.util.function.Supplier<WriterSession> creator) {
        return sessions.computeIfAbsent(threadId, k -> creator.get());
    }

    public void removeSession(String threadId) {
        sessions.remove(threadId);
    }
}
