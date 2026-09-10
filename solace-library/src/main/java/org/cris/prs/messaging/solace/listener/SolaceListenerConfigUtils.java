package org.cris.prs.messaging.solace.listener;

/** Well-known bean names for the {@code @SolaceListener} infrastructure. */
public final class SolaceListenerConfigUtils {

    private SolaceListenerConfigUtils() {
    }

    /** Bean name of the {@code @SolaceListener} annotation post-processor. */
    public static final String SOLACE_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME =
            "org.cris.prs.solace.autoconfigure.internalSolaceListenerAnnotationProcessor";

    /** Bean name of the registry holding every listener container. */
    public static final String SOLACE_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME =
            "org.cris.prs.solace.autoconfigure.internalSolaceListenerEndpointRegistry";

    /** Bean name of the container factory used when a listener names none. */
    public static final String DEFAULT_SOLACE_LISTENER_CONTAINER_FACTORY_BEAN_NAME =
            "solaceListenerContainerFactory";
}
