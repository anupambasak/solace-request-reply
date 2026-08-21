package cris.prs.messaging.solace.annotation;

import cris.prs.solace.autoconfigure.SolaceBootstrapConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Enables detection of {@link SolaceListener} annotated methods, exactly as {@code @EnableKafka}
 * does for {@code @KafkaListener}.
 *
 * <p>Spring Boot applications do not normally need this annotation: the Solace auto-configuration
 * adds it when no listener infrastructure is present. Declare it explicitly when auto-configuration
 * is switched off or when building on plain Spring.</p>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(SolaceBootstrapConfiguration.class)
public @interface EnableSolace {
}
