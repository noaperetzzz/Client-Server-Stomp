package bgu.spl.net.srv;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import bgu.spl.net.impl.data.Database;


public class ConnectionsImpl <T> implements Connections <T> {

    private ConcurrentHashMap<Integer, ConnectionHandler<T>> connectionHandlerMap;

    private AtomicInteger messageId;

    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, String>> channelSubscriptions;

    public ConnectionsImpl (){
        this.connectionHandlerMap = new ConcurrentHashMap<>();
        this.channelSubscriptions = new ConcurrentHashMap<>();
        this.messageId = new AtomicInteger(0);
    }

    public void addClient(int connectionId, ConnectionHandler<T> connectionHandler){
        connectionHandlerMap.put(connectionId, connectionHandler);
    }

    public void subscribe(int connectionId, String channel, String subscriptionId){
        channelSubscriptions.computeIfAbsent(channel, k -> new ConcurrentHashMap<Integer, String>()).put(connectionId, subscriptionId);
    }

    public void unsubscribe(int connectionId, String channel) {
        ConcurrentHashMap<Integer, String> channelUsers = channelSubscriptions.get(channel);
        channelUsers.remove(connectionId);
            
        if (channelUsers.isEmpty()) {
            channelSubscriptions.remove(channel);
        }
    }

    public boolean isConnected (int connectionId){
        return connectionHandlerMap.containsKey(connectionId);
    }


    @Override
    public boolean send(int connectionId, T msg) {
        ConnectionHandler<T> connectionHandler = connectionHandlerMap.get(connectionId);
        if (connectionHandler == null)
            return false;
        connectionHandler.send(msg);
        return true;
    }

    @Override
    public void send(String channel, T msg) {
        ConcurrentHashMap<Integer, String> mapUsers = channelSubscriptions.get(channel);
        if (mapUsers != null){
            for (Map.Entry<Integer, String> entry : mapUsers.entrySet()) {
                Integer connectionId = entry.getKey();
                String messageToReturn = "MESSAGE\nsubscription:" + entry.getValue() + "\nmessage-id:" + messageId.incrementAndGet() + "\ndestination:" + channel + "\n\n" + msg + "\n\u0000";
                send(connectionId, (T) messageToReturn);
        }
    }
    }

    @Override
    public void disconnect(int connectionId) {
        Database database = Database.getInstance();
        ConnectionHandler<T> handler = connectionHandlerMap.remove(connectionId);
        for (ConcurrentHashMap<Integer,String> userInfo : channelSubscriptions.values()){
            userInfo.remove(connectionId);
        }
        database.logout(connectionId);
    }

}
