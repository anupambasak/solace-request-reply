package cris.prs.messaging.solace.listener;

import com.solacesystems.jcsmp.BytesXMLMessage;
import cris.prs.messaging.solace.core.SolaceHeaderMapper;
import cris.prs.messaging.solace.core.SolaceHeaders;
import cris.prs.messaging.solace.core.SolaceMessageConverter;
import cris.prs.messaging.solace.core.SolaceRecord;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.messaging.support.GenericMessage;

import java.util.Map;

/**
 * Invokes a {@code @SolaceListener} annotated method, supporting the usual Spring Messaging
 * argument types ({@code @Payload}, {@code @Header}, {@code @Headers}, {@code Message<?>}) plus
 * {@link SolaceRecord} and the raw {@code BytesXMLMessage}.
 */
public class MethodSolaceListenerAdapter extends AbstractSolaceListenerAdapter {

    private final InvocableHandlerMethod handlerMethod;

    public MethodSolaceListenerAdapter(InvocableHandlerMethod handlerMethod,
            SolaceMessageConverter messageConverter, SolaceHeaderMapper headerMapper) {
        super(messageConverter, headerMapper);
        this.handlerMethod = handlerMethod;
    }

    @Override
    public void onMessage(BytesXMLMessage message) throws Exception {
        Object payload = convertPayload(message);
        Map<String, Object> headers = this.headerMapper.toHeaders(message);
        headers.put(SolaceHeaders.RAW_MESSAGE, message);
        SolaceRecord<Object> record = toRecord(payload, message, headers);
        Message<?> springMessage = new GenericMessage<>(payload != null ? payload : new byte[0], headers);
        Object result = this.handlerMethod.invoke(springMessage, message, record);
        handleResult(result, message);
    }
}
