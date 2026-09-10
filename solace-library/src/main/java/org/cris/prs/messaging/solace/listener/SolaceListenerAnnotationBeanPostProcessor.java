package org.cris.prs.messaging.solace.listener;

import com.solacesystems.jcsmp.BytesXMLMessage;
import org.cris.prs.messaging.solace.annotation.SolaceListener;
import org.cris.prs.messaging.solace.core.EndpointMode;
import org.cris.prs.messaging.solace.core.ExchangePattern;
import org.cris.prs.messaging.solace.core.ReplayStartPoint;
import org.cris.prs.messaging.solace.core.SettlementOutcome;
import org.cris.prs.messaging.solace.core.SolaceRecord;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Finds {@link SolaceListener} annotated methods on singleton beans and turns each one into a
 * listener container, mirroring {@code KafkaListenerAnnotationBeanPostProcessor}.
 */
@Slf4j
public class SolaceListenerAnnotationBeanPostProcessor
        implements BeanPostProcessor, BeanFactoryAware, SmartInitializingSingleton, Ordered {

    /** Create the annotation post-processor. Registered as an infrastructure bean by {@code @EnableSolace}. */
    public SolaceListenerAnnotationBeanPostProcessor() {
    }


    private final List<ListenerMethod> listenerMethods = new ArrayList<>();

    private final AtomicInteger counter = new AtomicInteger();

    private BeanFactory beanFactory;

    private DefaultMessageHandlerMethodFactory handlerMethodFactory;

    /**
     * {@inheritDoc}
     *
     * @return {@code LOWEST_PRECEDENCE}, so beans are fully initialised before being scanned
     */
    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }

    /**
     * {@inheritDoc}
     *
     * <p>The bean factory is used to resolve property placeholders in annotation attributes and to
     * look up container factories and the endpoint registry.</p>
     */
    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }

    /**
     * Collect {@code @SolaceListener} methods on this bean.
     *
     * <p>Registration is deferred to {@link #afterSingletonsInstantiated()} so that container
     * factories and the registry are guaranteed to exist. Methods are looked up on the target class,
     * so proxied beans are handled.</p>
     *
     * @param bean     the initialised bean
     * @param beanName its name
     * @return the bean, unchanged
     */
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

    /**
     * Turn every collected method into a registered listener container.
     *
     * <p>Attribute placeholders are resolved, the payload type derived from the method signature, the
     * exchange pattern's defaults applied last, and the endpoint registered with its container
     * factory.</p>
     *
     * @throws IllegalStateException if a listener declares neither topics nor a queue, or no container
     *                               factory can be resolved
     */
    @Override
    public void afterSingletonsInstantiated() {
        this.handlerMethodFactory = new DefaultMessageHandlerMethodFactory();
        this.handlerMethodFactory.setBeanFactory(this.beanFactory);
        this.handlerMethodFactory.afterPropertiesSet();

        SolaceListenerEndpointRegistry registry = this.beanFactory.getBean(
                SolaceListenerConfigUtils.SOLACE_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME,
                SolaceListenerEndpointRegistry.class);

        // Listeners that opted into topic dispatch are merged into one endpoint per queue+group;
        // everything else registers one endpoint per method, exactly as before.
        Map<String, List<ListenerMethod>> dispatchGroups = new LinkedHashMap<>();
        List<ListenerMethod> standalone = new ArrayList<>();
        for (ListenerMethod listenerMethod : this.listenerMethods) {
            String key = topicDispatchKey(listenerMethod);
            if (key == null) {
                standalone.add(listenerMethod);
            }
            else {
                dispatchGroups.computeIfAbsent(key, ignored -> new ArrayList<>()).add(listenerMethod);
            }
        }

        standalone.forEach(listenerMethod -> register(listenerMethod, registry));
        dispatchGroups.forEach((key, group) -> registerDispatchGroup(key, group, registry));
        this.listenerMethods.clear();
    }

    /**
     * The dispatch group a listener belongs to, or {@code null} when it wants its own endpoint.
     *
     * <p>Keyed by the resolved queue, group and container factory, since those are what decide which
     * physical endpoint a listener would have bound on its own.</p>
     *
     * @param listenerMethod the annotated method
     * @return the group key, or {@code null} when {@code topicDispatch} is not set
     */
    private String topicDispatchKey(ListenerMethod listenerMethod) {
        SolaceListener annotation = listenerMethod.annotation();
        if (!Boolean.TRUE.equals(resolveBoolean(annotation.topicDispatch()))) {
            return null;
        }
        String queue = resolve(annotation.queue());
        Assert.state(StringUtils.hasText(queue),
                "@SolaceListener on " + listenerMethod.method() + " sets topicDispatch but declares "
                        + "no queue; a dispatch group is identified by the endpoint it shares");
        return queue + '|' + resolve(annotation.group()) + '|' + resolve(annotation.containerFactory());
    }

    /**
     * Register one endpoint shared by every listener in a dispatch group.
     *
     * <p>Container settings come from the first listener; a later one that states a different
     * {@code pattern}, {@code endpointMode}, {@code concurrency}, {@code transactional} or
     * {@code selector} fails startup rather than having its setting silently dropped. Subscriptions
     * are the union, in declaration order, which is also the order the dispatcher matches in.</p>
     *
     * @param key      the group key, for error messages
     * @param group    the listeners sharing the endpoint, in declaration order
     * @param registry where the container is registered
     */
    private void registerDispatchGroup(String key, List<ListenerMethod> group,
            SolaceListenerEndpointRegistry registry) {
        ListenerMethod first = group.get(0);
        SolaceListenerEndpoint endpoint = buildEndpoint(first);
        endpoint.setInvocableHandlerMethod(null);

        List<String> topics = new ArrayList<>(endpoint.getTopics());
        endpoint.setDispatchTargets(new ArrayList<>());
        endpoint.getDispatchTargets().add(dispatchTarget(first));

        for (ListenerMethod listenerMethod : group.subList(1, group.size())) {
            SolaceListenerEndpoint candidate = buildEndpoint(listenerMethod);
            assertCompatible(first, listenerMethod, endpoint, candidate);
            candidate.getTopics().stream().filter(topic -> !topics.contains(topic)).forEach(topics::add);
            endpoint.getDispatchTargets().add(dispatchTarget(listenerMethod));
        }

        endpoint.setTopics(topics);
        Assert.state(!topics.isEmpty(), "The topic dispatch group '" + key
                + "' declares no topics; there would be nothing to route by");

        SolaceListenerContainerFactory factory = resolveContainerFactory(
                resolve(first.annotation().containerFactory()), first);
        registry.registerListenerContainer(endpoint, factory);
        log.debug("Registered topic-dispatching container '{}' for {} listener(s) on {}",
                endpoint.getId(), group.size(), topics);
    }

    /**
     * Build one target in a dispatch group's routing table.
     *
     * @param listenerMethod the annotated method
     * @return the target
     */
    private TopicDispatchTarget dispatchTarget(ListenerMethod listenerMethod) {
        SolaceListener annotation = listenerMethod.annotation();
        TopicDispatchTarget target = new TopicDispatchTarget();
        target.setTopics(Arrays.stream(annotation.topics()).map(this::resolve).toList());
        target.setPayloadType(resolvePayloadType(listenerMethod.method()));
        target.setReplyDestination(resolve(annotation.replyDestination()));
        target.setInvocableHandlerMethod(this.handlerMethodFactory
                .createInvocableHandlerMethod(listenerMethod.bean(), listenerMethod.method()));
        target.setDescription(listenerMethod.beanName() + '.' + listenerMethod.method().getName());
        return target;
    }

    /**
     * Fail startup when two listeners in a dispatch group disagree about the endpoint they share.
     *
     * <p>They bind one flow set between them, so a setting stated on the second listener could only
     * be honoured by ignoring the first. Saying so is better than picking one silently.</p>
     *
     * @param first      the listener whose settings the group uses
     * @param candidate  the listener being merged in
     * @param merged     the endpoint built from {@code first}
     * @param proposed   the endpoint built from {@code candidate}
     */
    private void assertCompatible(ListenerMethod first, ListenerMethod candidate,
            SolaceListenerEndpoint merged, SolaceListenerEndpoint proposed) {
        assertSame(first, candidate, "pattern", merged.getPattern(), proposed.getPattern());
        assertSame(first, candidate, "endpointMode", merged.getEndpointMode(), proposed.getEndpointMode());
        assertSame(first, candidate, "concurrency", merged.getConcurrency(), proposed.getConcurrency());
        assertSame(first, candidate, "transactional", merged.getTransactional(), proposed.getTransactional());
        assertSame(first, candidate, "selector", merged.getSelector(), proposed.getSelector());
        assertSame(first, candidate, "accessType", merged.getAccessType(), proposed.getAccessType());
    }

    /**
     * Assert that one setting agrees across a dispatch group.
     *
     * @param first     the listener whose settings the group uses
     * @param candidate the listener being merged in
     * @param attribute the attribute's name, for the error message
     * @param expected  the group's value
     * @param actual    the candidate's value
     */
    private void assertSame(ListenerMethod first, ListenerMethod candidate, String attribute,
            Object expected, Object actual) {
        Assert.state(Objects.equals(expected, actual),
                "@SolaceListener on " + candidate.method() + " sets " + attribute + "=" + actual
                        + ", but it shares a topic dispatch endpoint with " + first.method()
                        + " which uses " + expected + ". Listeners sharing an endpoint share its "
                        + "flows, so they must agree on how it is bound.");
    }

    private void register(ListenerMethod listenerMethod, SolaceListenerEndpointRegistry registry) {
        SolaceListener annotation = listenerMethod.annotation();
        SolaceListenerEndpoint endpoint = buildEndpoint(listenerMethod);

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
     * Build the endpoint an annotated method describes, without the handler method.
     *
     * <p>Shared by the ordinary one-endpoint-per-method path and by topic dispatch, which builds one
     * of these per member of a group so it can compare them before merging.</p>
     *
     * @param listenerMethod the annotated method
     * @return the endpoint, with every attribute resolved and the pattern's defaults applied
     */
    private SolaceListenerEndpoint buildEndpoint(ListenerMethod listenerMethod) {
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
        endpoint.setReplayFrom(ReplayStartPoint.parse(resolve(annotation.replayFrom())));

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
        String errorOutcome = resolve(annotation.errorOutcome());
        if (StringUtils.hasText(errorOutcome)) {
            endpoint.setErrorOutcome(SettlementOutcome.valueOf(errorOutcome.trim().toUpperCase()));
        }
        endpoint.setConcurrency(resolveInteger(annotation.concurrency()));
        endpoint.setTransactional(resolveBoolean(annotation.transactional()));
        endpoint.setAutoStartup(resolveBoolean(annotation.autoStartup()));
        endpoint.setAppendInstanceIdToQueue(resolveBoolean(annotation.appendInstanceIdToQueue()));
        endpoint.setAppendInstanceIdToTopics(resolveBoolean(annotation.appendInstanceIdToTopics()));

        // Last, so that anything stated explicitly above wins over the pattern's defaults.
        endpoint.applyPatternDefaults();
        return endpoint;
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
                + "org.cris.prs.solace.autoconfigure package.");
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
