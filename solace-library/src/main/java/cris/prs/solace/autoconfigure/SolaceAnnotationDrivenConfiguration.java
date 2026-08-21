package cris.prs.solace.autoconfigure;

import cris.prs.messaging.solace.annotation.EnableSolace;
import cris.prs.messaging.solace.listener.SolaceListenerAnnotationBeanPostProcessor;
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
}
