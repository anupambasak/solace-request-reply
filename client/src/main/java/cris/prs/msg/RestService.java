package cris.prs.msg;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class RestService {

    @Autowired
    private RequestReplyService rrs;


    @GetMapping("/send")
    public void ss(@RequestParam("cmd") String cmd){
        if(cmd == null || "".equals(cmd)){
            rrs.sendAndReceive("sb hello");
        }else{
            rrs.sendAndReceive(cmd);
        }
    }
}
