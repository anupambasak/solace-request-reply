package cris.prs.solace.autoconfigure;

import cris.prs.messaging.solace.listener.SolaceListenerAnnotationBeanPostProcessor;
import cris.prs.messaging.solace.listener.SolaceListenerConfigUtils;
import cris.prs.messaging.solace.listener.SolaceListenerEndpointRegistry;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

/**
 * Registers the {@code @SolaceListener} infrastructure beans; imported by
 * {@link cris.prs.messaging.solace.annotation.EnableSolace}.
 */
public class SolaceBootstrapConfiguration implements ImportBeanDefinitionRegistrar {

    /** Create the registrar. Imported by {@code @EnableSolace}, not instantiated by application code. */
    public SolaceBootstrapConfiguration() {
    }


    /**
     * Register the annotation processor and endpoint registry as infrastructure beans.
     *
     * <p>Both registrations are guarded by a presence check, so importing {@code @EnableSolace} more
     * than once is harmless.</p>
     *
     * @param importingClassMetadata metadata of the class carrying {@code @EnableSolace}
     * @param registry               the registry to add the definitions to
     */
    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        if (!registry.containsBeanDefinition(
                SolaceListenerConfigUtils.SOLACE_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)) {
            registry.registerBeanDefinition(
                    SolaceListenerConfigUtils.SOLACE_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME,
                    infrastructureBeanDefinition(SolaceListenerAnnotationBeanPostProcessor.class));
        }
        if (!registry.containsBeanDefinition(
                SolaceListenerConfigUtils.SOLACE_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME)) {
            registry.registerBeanDefinition(
                    SolaceListenerConfigUtils.SOLACE_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME,
                    infrastructureBeanDefinition(SolaceListenerEndpointRegistry.class));
        }
    }

    private static RootBeanDefinition infrastructureBeanDefinition(Class<?> type) {
        RootBeanDefinition beanDefinition = new RootBeanDefinition(type);
        beanDefinition.setRole(BeanDefinition.ROLE_INFRASTRUCTURE);
        return beanDefinition;
    }
}
