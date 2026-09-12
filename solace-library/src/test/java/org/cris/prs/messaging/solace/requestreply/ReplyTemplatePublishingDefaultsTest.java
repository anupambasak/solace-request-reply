package org.cris.prs.messaging.solace.requestreply;

import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import org.cris.prs.messaging.solace.core.DefaultSolaceHeaderMapper;
import org.cris.prs.messaging.solace.core.JacksonSolaceMessageConverter;
import org.cris.prs.messaging.solace.core.SolaceSessionFactory;
import org.cris.prs.messaging.solace.listener.SolaceMessageListener;
import org.cris.prs.messaging.solace.listener.SolaceMessageListenerContainer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A reply template an application declares publishes like {@code solaceTemplate} does.
 *
 * <p>Expiry, priority and DMQ eligibility used to stop at the auto-configured template: a
 * {@code ReplyEndpointSpec} could not state them and the factory did not pass {@code solace.template.*}
 * on, so a service given its own reply destination quietly published requests that never expire. That
 * matters most on a request-reply template, where a request whose requester has already timed out should
 * stop being delivered rather than be answered minutes later.</p>
 */
class ReplyTemplatePublishingDefaultsTest {

    /** Nothing here connects: the factory only needs a session factory to hand to the template. */
    private static final SolaceSessionFactory SESSIONS = new SolaceSessionFactory() {

        @Override
        public JCSMPSession getSharedSession() {
            return null;
        }

        @Override
        public JCSMPSession createSession() {
            return null;
        }

        @Override
        public XMLMessageProducer getSharedProducer() {
            return null;
        }

        @Override
        public XMLMessageProducer getProducer(JCSMPSession session) {
            return null;
        }

        @Override
        public TransactedSession createTransactedSession() {
            return null;
        }

        @Override
        public TransactedSession createTransactedSession(JCSMPSession session) {
            return null;
        }

        @Override
        public void closeSession(JCSMPSession session) {
        }
    };

    /** A reply container that is never started; the factory's own would try to bind an endpoint. */
    private static final class NoOpContainer implements SolaceMessageListenerContainer {

        private final String id;

        private NoOpContainer(String id) {
            this.id = id;
        }

        @Override
        public String getListenerId() {
            return this.id;
        }

        @Override
        public void setupMessageListener(SolaceMessageListener listener) {
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public boolean isRunning() {
            return false;
        }
    }

    private static ReplyingSolaceTemplateFactory factory() {
        ReplyingSolaceTemplateFactory factory = new ReplyingSolaceTemplateFactory(SESSIONS,
                new JacksonSolaceMessageConverter(), new DefaultSolaceHeaderMapper(), () -> "pod-1") {

            @Override
            protected SolaceMessageListenerContainer createReplyContainer(ReplyEndpointSpec spec,
                    String instanceId) {
                return new NoOpContainer(spec.getId());
            }
        };
        // What auto-configuration copies out of solace.template.*
        factory.setDefaultTimeToLive(30_000L);
        factory.setDefaultPriority(4);
        factory.setDefaultDmqEligible(true);
        return factory;
    }

    @Test
    @DisplayName("a spec that states nothing inherits the solace.template.* publishing defaults")
    void inherited() {
        ReplyingSolaceTemplate template = factory().create(new ReplyEndpointSpec());

        assertEquals(30_000L, template.getTimeToLive());
        assertEquals(4, template.getPriority());
        assertTrue(template.isDmqEligible());
    }

    @Test
    @DisplayName("a spec that states them wins over the defaults")
    void overridden() {
        ReplyEndpointSpec spec = new ReplyEndpointSpec();
        spec.setTimeToLive(5_000L);
        spec.setPriority(9);
        spec.setDmqEligible(false);

        ReplyingSolaceTemplate template = factory().create(spec);

        assertEquals(5_000L, template.getTimeToLive());
        assertEquals(9, template.getPriority());
        assertFalse(template.isDmqEligible());
    }

    @Test
    @DisplayName("zero expiry stays zero: a spec may switch expiry off where the defaults set one")
    void expiryCanBeTurnedOff() {
        ReplyEndpointSpec spec = new ReplyEndpointSpec();
        spec.setTimeToLive(0L);

        assertEquals(0L, factory().create(spec).getTimeToLive());
    }

    @Test
    @DisplayName("a factory nobody configured publishes as it always did: no expiry, no priority, DMQ on")
    void factoryDefaults() {
        ReplyingSolaceTemplateFactory bare = new ReplyingSolaceTemplateFactory(SESSIONS,
                new JacksonSolaceMessageConverter(), new DefaultSolaceHeaderMapper(), () -> "pod-1") {

            @Override
            protected SolaceMessageListenerContainer createReplyContainer(ReplyEndpointSpec spec,
                    String instanceId) {
                return new NoOpContainer(spec.getId());
            }
        };

        ReplyingSolaceTemplate template = bare.create(new ReplyEndpointSpec());

        assertEquals(0L, template.getTimeToLive());
        assertNull(template.getPriority());
        assertTrue(template.isDmqEligible());
    }
}
