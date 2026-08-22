package cris.prs.messaging.solace.listener;

import com.solacesystems.jcsmp.BytesXMLMessage;
import cris.prs.messaging.solace.annotation.SolaceListener;
import cris.prs.messaging.solace.core.EndpointMode;
import cris.prs.messaging.solace.core.ExchangePattern;
import cris.prs.messaging.solace.core.SolaceRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.Ordered;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Finds {@link SolaceListener} annotated methods on singleton beans and turns each one into a
 * listener container, mirroring {@code KafkaListenerAnnotationBeanPostProcessor}.
 */
@Slf4j
public class SolaceListenerAnnotationBeanPostProcessor
        implements BeanPostProcessor, BeanFactoryAware, SmartInitializingSingleton, Ordered {

    private final List<ListenerMethod> listenerMethods = new ArrayList<>();

    private final AtomicInteger counter = new AtomicInteger();

    private BeanFactory beanFactory;

    private DefaultMessageHandlerMethodFactory handlerMethodFactory;

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        MethodIntrospector.selectMethods(targetClass,
                (MethodIntrospector.MetadataLookup<SolaceListener>) method ->
                        AnnotatedElementUtils.findMergedAnnotation(method, SolaceListener.class))
                .forEach((method, annotation) ->
                        this.listenerMethods.add(new ListenerMethod(bean, beanName, method, annotation)));
        return bean;
    }

    @Override
    public void afterSingletonsInstantiated() {
        this.handlerMethodFactory = new DefaultMessageHandlerMethodFactory();
        this.handlerMethodFactory.setBeanFactory(this.beanFactory);
        this.handlerMethodFactory.afterPropertiesSet();

        SolaceListenerEndpointRegistry registry = this.beanFactory.getBean(
                SolaceListenerConfigUtils.SOLACE_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME,
                SolaceListenerEndpointRegistry.class);

        this.listenerMethods.forEach(listenerMethod -> register(listenerMethod, registry));
        this.listenerMethods.clear();
    }

    private void register(ListenerMethod listenerMethod, SolaceListenerEndpointRegistry registry) {
        SolaceListener annotation = listenerMethod.annotation();
        SolaceListenerEndpoint endpoint = new SolaceListenerEndpoint();

        String id = resolve(annotation.id());
        endpoint.setId(StringUtils.hasText(id) ? id
                : "solaceListenerEndpoint#" + this.counter.getAndIncrement());
        endpoint.setTopics(Arrays.stream(annotation.topics()).map(this::resolve).toList());
        endpoint.setQueue(resolve(annotation.queue()));
        endpoint.setGroup(resolve(annotation.group()));
        endpoint.setSelector(resolve(annotation.selector()));
        endpoint.setReplyDestination(resolve(annotation.replyDestination()));

        String pattern = resolve(annotation.pattern());
        if (StringUtils.hasText(pattern)) {
            endpoint.setPattern(ExchangePattern.valueOf(pattern.trim().toUpperCase()));
        }

        String endpointMode = resolve(annotation.endpointMode());
        if (StringUtils.hasText(endpointMode)) {
            endpoint.setEndpointMode(EndpointMode.valueOf(endpointMode.trim().toUpperCase()));
        }
        String dispatch = resolve(annotation.dispatch());
        if (StringUtils.hasText(dispatch)) {
            endpoint.setDispatch(ContainerProperties.DispatchMode.valueOf(dispatch.trim().toUpperCase()));
        }
        endpoint.setConcurrency(resolveInteger(annotation.concurrency()));
        endpoint.setTransactional(resolveBoolean(annotation.transactional()));
        endpoint.setAutoStartup(resolveBoolean(annotation.autoStartup()));
        endpoint.setAppendInstanceIdToQueue(resolveBoolean(annotation.appendInstanceIdToQueue()));
        endpoint.setAppendInstanceIdToTopics(resolveBoolean(annotation.appendInstanceIdToTopics()));

        // Last, so that anything stated explicitly above wins over the pattern's defaults.
        endpoint.applyPatternDefaults();

        endpoint.setPayloadType(resolvePayloadType(listenerMethod.method()));
        InvocableHandlerMethod handlerMethod = this.handlerMethodFactory
                .createInvocableHandlerMethod(listenerMethod.bean(), listenerMethod.method());
        endpoint.setInvocableHandlerMethod(handlerMethod);

        SolaceListenerContainerFactory factory = resolveContainerFactory(
                resolve(annotation.containerFactory()), listenerMethod);
        Assert.state(!endpoint.getTopics().isEmpty() || StringUtils.hasText(endpoint.getQueue()),
                "@SolaceListener on " + listenerMethod.method() + " must declare topics or a queue");

        registry.registerListenerContainer(endpoint, factory);
        log.debug("Registered Solace listener container '{}' for {}.{}", endpoint.getId(),
                listenerMethod.beanName(), listenerMethod.method().getName());
    }

    /**
     * Look the container factory up by name, falling back to a unique bean of the right type. The
     * fallback matters because an application that component scans the auto-configuration package
     * can end up with the annotation processor but not the auto-configured factory.
     */
    private SolaceListenerContainerFactory resolveContainerFactory(String factoryName,
            ListenerMethod listenerMethod) {
        String name = StringUtils.hasText(factoryName)
                ? factoryName
                : SolaceListenerConfigUtils.DEFAULT_SOLACE_LISTENER_CONTAINER_FACTORY_BEAN_NAME;
        if (this.beanFactory.containsBean(name)) {
            return this.beanFactory.getBean(name, SolaceListenerContainerFactory.class);
        }
        if (this.beanFactory instanceof ListableBeanFactory listableBeanFactory) {
            Map<String, SolaceListenerContainerFactory> candidates =
                    listableBeanFactory.getBeansOfType(SolaceListenerContainerFactory.class);
            if (candidates.size() == 1) {
                return candidates.values().iterator().next();
            }
        }
        throw new IllegalStateException("No SolaceListenerContainerFactory named '" + name
                + "' is available for @SolaceListener on " + listenerMethod.method()
                + ". The Solace auto-configuration did not run: check that solace.java.host is "
                + "configured and that the application does not component scan the "
                + "cris.prs.solace.autoconfigure package.");
    }

    /**
     * Work out the type the message body should be converted into: the first parameter that is not
     * a framework type and is not annotated with {@code @Header}/{@code @Headers}, unwrapping
     * {@code Message<T>} and {@code SolaceRecord<T>}.
     */
    private Class<?> resolvePayloadType(Method method) {
        Parameter[] parameters = method.getParameters();
        for (int i = 0; i < parameters.length; i++) {
            Parameter parameter = parameters[i];
            Class<?> type = parameter.getType();
            if (parameter.isAnnotationPresent(Header.class) || parameter.isAnnotationPresent(Headers.class)
                    || MessageHeaders.class.isAssignableFrom(type)
                    || BytesXMLMessage.class.isAssignableFrom(type)) {
                continue;
            }
            if (Message.class.isAssignableFrom(type) || SolaceRecord.class.isAssignableFrom(type)) {
                ResolvableType generic = ResolvableType.forMethodParameter(method, i).getGeneric(0);
                Class<?> resolved = generic.resolve();
                return resolved != null ? resolved : Object.class;
            }
            return type;
        }
        return Object.class;
    }

    private String resolve(String value) {
        if (!StringUtils.hasText(value)) {
            return value;
        }
        if (this.beanFactory instanceof ConfigurableBeanFactory configurableBeanFactory) {
            return configurableBeanFactory.resolveEmbeddedValue(value);
        }
        return value;
    }

    private Integer resolveInteger(String value) {
        String resolved = resolve(value);
        return StringUtils.hasText(resolved) ? Integer.valueOf(resolved.trim()) : null;
    }

    private Boolean resolveBoolean(String value) {
        String resolved = resolve(value);
        return StringUtils.hasText(resolved) ? Boolean.valueOf(resolved.trim()) : null;
    }

    /** A discovered {@code @SolaceListener} method, pending registration. */
    private record ListenerMethod(Object bean, String beanName, Method method, SolaceListener annotation) {
    }
}
