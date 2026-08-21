package cris.prs.messaging.spring.config;

import cris.prs.messaging.solace.transaction.SolaceTransactionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
public class AppConfig {

    /** Programmatic transaction support over the auto-configured Solace transaction manager. */
    @Bean
    public TransactionTemplate transactionTemplate(SolaceTransactionManager solaceTransactionManager) {
        return new TransactionTemplate(solaceTransactionManager);
    }
}
