package cris.prs.messaging.solace.listener;

/** Well-known bean names for the {@code @SolaceListener} infrastructure. */
public final class SolaceListenerConfigUtils {

    private SolaceListenerConfigUtils() {
    }

    public static final String SOLACE_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME =
            "cris.prs.solace.autoconfigure.internalSolaceListenerAnnotationProcessor";

    public static final String SOLACE_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME =
            "cris.prs.solace.autoconfigure.internalSolaceListenerEndpointRegistry";

    public static final String DEFAULT_SOLACE_LISTENER_CONTAINER_FACTORY_BEAN_NAME =
            "solaceListenerContainerFactory";
}
