package org.cris.prs.solace.autoconfigure;

import org.cris.prs.messaging.solace.annotation.EnableSolace;
import org.cris.prs.messaging.solace.listener.SolaceListenerAnnotationBeanPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Configuration;

/**
 * Turns on {@code @SolaceListener} detection unless the application has already declared
 * {@link EnableSolace} itself &mdash; mirroring Spring Boot's
 * {@code KafkaAnnotationDrivenConfiguration}.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnMissingBean(SolaceListenerAnnotationBeanPostProcessor.class)
@EnableSolace
public class SolaceAnnotationDrivenConfiguration {

    /** Create the configuration. Instantiated by Spring Boot, not by application code. */
    public SolaceAnnotationDrivenConfiguration() {
    }

}
