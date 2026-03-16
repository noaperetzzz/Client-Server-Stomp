package bgu.spl.net.api;

import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import bgu.spl.net.impl.data.Database;
import bgu.spl.net.impl.data.LoginStatus;
import bgu.spl.net.srv.Connections;
import bgu.spl.net.srv.ConnectionsImpl;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class StompMessagingProtocolImpl implements StompMessagingProtocol<String>{

    private boolean shouldTerminate = false;
    private int connectionId;
    private ConnectionsImpl<String> connections;
    private ConcurrentHashMap <String, String> mySubscriptions = new ConcurrentHashMap<>();

    @Override
    public void start(int connectionId, Connections connections) {
        this.connectionId = connectionId;
        this.connections = (ConnectionsImpl) connections;
    }

    @Override
    public void process(String message) {
        String[] lines = message.split("\n");
        if (lines.length == 0) return;

        String command = lines[0].trim(); 
        Database database = Database.getInstance();
        String messageToReturn = null;
        String receipt = getHeader(lines, "receipt");

        if (command.equals("CONNECT")) {
            String userName = getHeader(lines, "login");
            String password = getHeader(lines, "passcode");

            LoginStatus status = database.login(connectionId, userName, password);

            if (status == LoginStatus.LOGGED_IN_SUCCESSFULLY || status == LoginStatus.ADDED_NEW_USER) {
                messageToReturn = "CONNECTED\nversion:1.2\n\n\u0000";
                connections.send(connectionId, messageToReturn);
            } else {
                messageToReturn = errorCondition(receipt, "Login failed: " + status, message);
                connections.send(connectionId, messageToReturn);
                shouldTerminate = true;
                connections.disconnect(connectionId);
            }
        }

        else if (command.equals("DISCONNECT")){
            if (receipt != null){
                messageToReturn = "RECEIPT\nreceipt-id:" + receipt + "\n\n\u0000";
                connections.send(connectionId, messageToReturn);
            }
            shouldTerminate = true;
            connections.disconnect(connectionId);
        }

        else if (command.equals("SEND")){
            String destination = getHeader(lines, "destination");
            boolean isSubscribed = mySubscriptions.containsValue(destination);

            if (destination != null && isSubscribed){
                int bodyStart = message.indexOf("\n\n");
        
                if (bodyStart != -1) {
                    String messageToChannel = message.substring(bodyStart + 2);
                    if (messageToChannel.contains("file name: ") && messageToChannel.contains("user: ")){
                        int index_of_file = messageToChannel.indexOf("file name: ");
                        int index_of_user = messageToChannel.indexOf("user: ");

                        String fileNamePart = messageToChannel.substring(index_of_file + 11);
                        String fileName = fileNamePart.split("\n")[0].trim().replace("\u0000", "");

                        String userPart = messageToChannel.substring(index_of_user + 6);
                        String name_of_user = userPart.split("\n")[0].trim().replace("\u0000", "");

                        String sql = "INSERT INTO reports (username, filename, game_channel, timestamp) " + "VALUES ('" + name_of_user + "', '" + fileName + "', '" + destination + "', DATETIME('now'))";
                        sendToSqlDatabase(sql); 

                        messageToChannel = messageToChannel.substring(0, index_of_file).trim() + "\u0000";
                    }
                    else {
                        messageToChannel = messageToChannel.replace("\u0000", "");
                    }
                    connections.send(destination, messageToChannel);
                } else {
                    messageToReturn = errorCondition(receipt, "Send failed", message);
                    connections.send(connectionId, messageToReturn);
                    shouldTerminate = true;
                    connections.disconnect(connectionId);

                }

                if (receipt != null){
                    messageToReturn = "RECEIPT\nreceipt-id:" + receipt + "\n\n\u0000";
                    connections.send(connectionId, messageToReturn);
                }
            }
            else {
                messageToReturn = errorCondition(receipt, "Send failed", message);
                connections.send(connectionId, messageToReturn);
                shouldTerminate = true;
                connections.disconnect(connectionId);
            }
    }

        else if (command.equals("SUBSCRIBE")){
            String subscriptionId = getHeader(lines, "id");
            String destination = getHeader(lines, "destination");
            if (!connections.isConnected(connectionId)){
                messageToReturn = errorCondition(receipt, "User is not connected", message);
                connections.send(connectionId, messageToReturn);
            }
            else {
                connections.subscribe(connectionId, destination, subscriptionId);
                mySubscriptions.put(subscriptionId, destination);
                if (receipt != null){
                    messageToReturn = "RECEIPT\nreceipt-id:" + receipt + "\n\n\u0000";
                    connections.send(connectionId, messageToReturn);
                }
            }
        }
        
        else if (command.equals("UNSUBSCRIBE")){
            String subscriptionId = getHeader(lines, "id");
            if (!connections.isConnected(connectionId)){
                messageToReturn = errorCondition(receipt, "User is not connected", message);
                connections.send(connectionId, messageToReturn);
                shouldTerminate = true;
                connections.disconnect(connectionId);
            }
            else if (!mySubscriptions.containsKey(subscriptionId)){
                messageToReturn = errorCondition(receipt, "SubscriptionId doesn't exists", message);
                connections.send(connectionId, messageToReturn);
                shouldTerminate = true;
                connections.disconnect(connectionId);
            }
            else {
                if (receipt != null){
                    messageToReturn = "RECEIPT\nreceipt-id:" + receipt + "\n\n\u0000";
                    connections.send(connectionId, messageToReturn);
                }
                String channel = mySubscriptions.get(subscriptionId);
                connections.unsubscribe(connectionId, channel);
                mySubscriptions.remove(subscriptionId);
            }
            
        }
        else {
            messageToReturn = errorCondition(receipt, "Command doesn'e exist", message);
            connections.send(connectionId, messageToReturn);
            shouldTerminate = true;
            connections.disconnect(connectionId);
        }

    }

    private String getHeader(String[] lines, String headerName) {
        for (String line : lines) {
            if (line.startsWith(headerName + ":")) {
                return line.substring(line.indexOf(":") + 1).trim();
            }
        }
        return null;
    }

    private String errorCondition(String receipt, String problem, String messageFromUser) {
        StringBuilder sb = new StringBuilder();
        sb.append("ERROR\n");
        if (receipt != null) {
            sb.append("receipt-id:").append(receipt).append("\n");
        }
        sb.append("message:").append(problem).append("\n\n");
        sb.append("The message:\n-----\n").append(messageFromUser).append("\n-----\n\u0000");
        return sb.toString();
    }

    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }

    private void sendToSqlDatabase(String sqlQuery) {
        try (Socket socket = new Socket("127.0.0.1", 7778);
            OutputStream out = socket.getOutputStream()) {
            
            String messageWithNull = sqlQuery + "\0";
            out.write(messageWithNull.getBytes(StandardCharsets.UTF_8));
            out.flush();
            
        } catch (Exception e) {
            System.out.println("Could not connect to SQL server: " + e.getMessage());
        }
    }
    
}
