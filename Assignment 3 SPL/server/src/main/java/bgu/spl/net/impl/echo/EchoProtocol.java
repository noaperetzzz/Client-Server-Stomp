package bgu.spl.net.impl.echo;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.Connections;
import java.time.LocalDateTime;

public class EchoProtocol implements StompMessagingProtocol<String> {

    private int connectionId;
    private Connections<String> connections;
    private boolean shouldTerminate = false;

    @Override
    public void start(int connectionId, Connections<String> connections) {
        this.connectionId = connectionId;
        this.connections = connections;
    }

    @Override
    public void process(String msg) {
        shouldTerminate = "bye".equals(msg);
        System.out.println("[" + LocalDateTime.now() + "]: " + msg);
        
        String echoPart = msg.substring(Math.max(msg.length() - 2, 0), msg.length());
        String response = msg + " .. " + echoPart + " .. " + echoPart + " ..";

        connections.send(connectionId, response);
    }

    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }
}
