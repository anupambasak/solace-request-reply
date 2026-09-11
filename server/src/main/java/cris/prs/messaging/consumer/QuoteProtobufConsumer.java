package cris.prs.messaging.consumer;

import cris.prs.messaging.proto.QuoteProtoMapper;
import cris.prs.messaging.proto.QuoteReply;
import cris.prs.messaging.proto.QuoteRequest;
import lombok.extern.slf4j.Slf4j;
import org.cris.prs.messaging.solace.annotation.SolaceListener;
import org.cris.prs.messaging.solace.core.SolaceHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * The quote service again, with request and reply as <b>Google Protocol Buffers</b> governed by Apicurio
 * Registry.
 *
 * <p>Protobuf has no reflection mode, so the wire types are the generated {@link QuoteRequest} and
 * {@link QuoteReply} from {@code shared-proto/src/main/proto/quote.proto}; the quote itself is still
 * computed on the shared {@code Person} and {@code Quote} DTOs, converted at the edge by
 * {@link QuoteProtoMapper}.</p>
 *
 * <p>A generated message needs no format setting: the converter recognises it and uses the Protobuf codec
 * wherever it is sent. The {@code topic-profile} mappings only name the registry artifacts &mdash;
 * {@code request-reply/quote-protobuf/request} for requests, {@code request-reply/quote-protobuf/reply/>}
 * for every client instance's replies. Inbound, Apicurio yields a {@code DynamicMessage}, which the codec
 * re-parses into the {@code QuoteRequest} this method asks for.</p>
 */
@Slf4j
@Component
public class QuoteProtobufConsumer {

    /**
     * Quote for the person in a Protobuf request, answered in Protobuf.
     *
     * @param request       decoded from Protobuf through the registry
     * @param correlationId the request's correlation id, for the log
     * @return the quote as a generated Protobuf message
     */
    @SolaceListener(
            id = "quoteProtobuf",
            pattern = "REQUEST_REPLY",
            queue = "${app.quote-protobuf.queue:request-reply-queue-5}",
            group = "${app.quote-protobuf.group:request-reply-group-5}",
            topics = "${app.quote-protobuf.topic:request-reply/quote-protobuf/request}",
            concurrency = "${app.quote-protobuf.concurrency:5}",
            transactional = "${app.quote-protobuf.transactional:true}")
    public QuoteReply quote(QuoteRequest request,
            @Header(name = SolaceHeaders.CORRELATION_ID, required = false) String correlationId) {
        log.debug("Handling Protobuf quote correlationId={} person={}", correlationId, request.getPersonName());
        return QuoteProtoMapper.toReply(QuotePricing.quoteFor(QuoteProtoMapper.toPerson(request)));
    }
}
