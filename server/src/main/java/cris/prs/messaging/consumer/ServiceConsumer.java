package cris.prs.messaging.consumer;

import cris.prs.messaging.Person;
import cris.prs.messaging.solace.annotation.SolaceListener;
import cris.prs.messaging.solace.core.SolaceHeaders;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Request handler.
 *
 * <p>The listener is bound to the durable queue {@code bkg.bkgGrp} with the topic subscriptions
 * {@code bkg/trn} and {@code bkg/trn/>}. The returned {@link Person} is published to the
 * destination carried in the request's {@code replyTo} field &mdash; the per-instance reply topic
 * of whichever client pod sent it &mdash; with the correlation id copied across.</p>
 *
 * <p>The container is transactional, so the acknowledgement of the request and the publication of
 * the reply commit as a single Solace local transaction: if this method throws, neither happens and
 * the broker redelivers.</p>
 */
@Slf4j
@Component
public class ServiceConsumer {

    @SolaceListener(
            id = "booking",
            queue = "${app.request.queue:bkg}",
            group = "${app.request.group:bkgGrp}",
            topics = {"${app.request.topic:bkg/trn}", "${app.request.topic:bkg/trn}/>"},
            concurrency = "${app.request.concurrency:10}",
            transactional = "${app.request.transactional:true}")
    public Person booking(Person person,
            @Header(name = SolaceHeaders.CORRELATION_ID, required = false) String correlationId) {
        log.debug("Handling booking correlationId={} payload={}", correlationId, person);
        person.setName(person.getName().toUpperCase());
        person.setAge(person.getAge() + 23);
        return person;
    }
}
