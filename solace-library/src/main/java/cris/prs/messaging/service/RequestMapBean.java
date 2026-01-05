package cris.prs.messaging.service;

import cris.prs.messaging.SolaceRequest;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class RequestMapBean<V> {

    private final Map<String, SolaceRequest<V>> outstandingRequests = new ConcurrentHashMap<>();

    public void put(String key, SolaceRequest<V> value){
        outstandingRequests.put(key, value);
    }

    public SolaceRequest<V> remove(String key){
        return outstandingRequests.remove(key);
    }
}
