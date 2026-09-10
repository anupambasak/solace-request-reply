package org.cris.prs.solace.autoconfigure;

import org.cris.prs.messaging.solace.core.SolaceSessionFactory;
import org.cris.prs.messaging.solace.listener.SolaceListenerEndpointRegistry;
import org.cris.prs.messaging.solace.listener.SolaceListenerMetrics;
import org.cris.prs.messaging.solace.observability.MicrometerSolaceListenerMetrics;
import org.cris.prs.messaging.solace.observability.MicrometerSolaceRequestReplyMetrics;
import org.cris.prs.messaging.solace.observability.SolaceHealthIndicator;
import org.cris.prs.messaging.solace.observability.SolaceMetricsBinder;
import org.cris.prs.messaging.solace.requestreply.ReplyingSolaceTemplate;
import org.cris.prs.messaging.solace.requestreply.SolaceRequestReplyMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Optional observability wiring: Micrometer meters and an Actuator health indicator.
 *
 * <p>Imported by {@link SolaceAutoConfiguration}, and entirely conditional. Neither Micrometer nor
 * Actuator is required for the library to work: without them the containers and templates keep their
 * no-op metrics collaborators and nothing here is registered.</p>
 *
 * <p>Split into two nested configurations so that each dependency is guarded separately &mdash; an
 * application may have Micrometer without Actuator, or the reverse.</p>
 *
 * @see org.cris.prs.messaging.solace.observability.SolaceMetricNames
 */
@Configuration(proxyBeanMethods = false)
public class SolaceObservabilityConfiguration {

    /** Create the configuration. Instantiated by Spring, not by application code. */
    public SolaceObservabilityConfiguration() {
    }

    /**
     * Micrometer instrumentation, active when a {@code MeterRegistry} bean exists and
     * {@code solace.metrics.enabled} is not {@code false}.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterRegistry.class)
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnProperty(prefix = "solace.metrics", name = "enabled", havingValue = "true",
            matchIfMissing = true)
    public static class MetricsConfiguration {

        /** Create the configuration. Instantiated by Spring, not by application code. */
        public MetricsConfiguration() {
        }

        /**
         * Records per-message counters and timers for every listener container.
         *
         * <p>Injected into the container factory, so every {@code @SolaceListener} reports through
         * it without any per-listener configuration.</p>
         *
         * @param meterRegistry where meters are published
         * @return the listener metrics collaborator
         */
        @Bean
        @ConditionalOnMissingBean
        public SolaceListenerMetrics solaceListenerMetrics(MeterRegistry meterRegistry) {
            return new MicrometerSolaceListenerMetrics(meterRegistry);
        }

        /**
         * Records request counters and round-trip latency for every request-reply template.
         *
         * <p>Injected into {@code ReplyingSolaceTemplateFactory}, so additional reply destinations
         * declared by the application are instrumented too.</p>
         *
         * @param meterRegistry where meters are published
         * @return the request-reply metrics collaborator
         */
        @Bean
        @ConditionalOnMissingBean
        public SolaceRequestReplyMetrics solaceRequestReplyMetrics(MeterRegistry meterRegistry) {
            return new MicrometerSolaceRequestReplyMetrics(meterRegistry);
        }

        /**
         * Registers the state gauges once every container and template has started.
         *
         * @param meterRegistry     where gauges are published
         * @param endpointRegistry  supplies the containers to sample
         * @param replyingTemplates every request-reply template in the context; may be empty
         * @param sessionFactory    sampled for session state and broker-side statistics
         * @param properties        supplies {@code solace.metrics.session-statistics}
         * @return the gauge binder
         */
        @Bean
        @ConditionalOnMissingBean
        public SolaceMetricsBinder solaceMetricsBinder(MeterRegistry meterRegistry,
                SolaceListenerEndpointRegistry endpointRegistry,
                ObjectProvider<ReplyingSolaceTemplate> replyingTemplates,
                SolaceSessionFactory sessionFactory, SolaceProperties properties) {
            return new SolaceMetricsBinder(meterRegistry, endpointRegistry,
                    replyingTemplates.orderedStream().toList(), sessionFactory,
                    properties.getMetrics().getSessionStatistics());
        }
    }

    /**
     * The Actuator health indicator, active when Actuator is on the classpath and
     * {@code solace.health.enabled} is not {@code false}.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(HealthIndicator.class)
    @ConditionalOnProperty(prefix = "solace.health", name = "enabled", havingValue = "true",
            matchIfMissing = true)
    public static class HealthConfiguration {

        /** Create the configuration. Instantiated by Spring, not by application code. */
        public HealthConfiguration() {
        }

        /**
         * Contributes {@code /actuator/health/solace}.
         *
         * <p>The bean name decides the key the indicator appears under, so it is deliberately
         * {@code solaceHealthIndicator}: Actuator strips the {@code HealthIndicator} suffix.</p>
         *
         * @param sessionFactory    asked whether its connection is usable
         * @param endpointRegistry  supplies the containers to report on
         * @param replyingTemplates every request-reply template in the context; may be empty
         * @param properties        supplies {@code solace.health.require-all-containers-running}
         * @return the health indicator
         */
        @Bean
        @ConditionalOnMissingBean(name = "solaceHealthIndicator")
        public SolaceHealthIndicator solaceHealthIndicator(SolaceSessionFactory sessionFactory,
                SolaceListenerEndpointRegistry endpointRegistry,
                ObjectProvider<ReplyingSolaceTemplate> replyingTemplates,
                SolaceProperties properties) {
            List<ReplyingSolaceTemplate> templates = replyingTemplates.orderedStream().toList();
            return new SolaceHealthIndicator(sessionFactory, endpointRegistry, templates,
                    properties.getHealth().isRequireAllContainersRunning());
        }
    }
}
