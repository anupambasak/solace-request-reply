package cris.prs.messaging;

import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class RequestMapBean {

    private final Map<String, PendingRequest> outstandingRequests = new ConcurrentHashMap<>();

    public void put(String key, PendingRequest value){
        outstandingRequests.put(key, value);
    }

    public PendingRequest remove(String key){
        return outstandingRequests.remove(key);
    }
}
