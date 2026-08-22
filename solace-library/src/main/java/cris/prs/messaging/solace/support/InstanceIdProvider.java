package cris.prs.messaging.solace.support;

/**
 * Supplies the identifier of this application instance, used to give every pod/host its own reply
 * destination so that request-reply works across a horizontally scaled deployment.
 */
@FunctionalInterface
public interface InstanceIdProvider {

    /**
     * @return the identifier of this instance; must be stable for the life of the application and
     *         safe to embed in a single Solace topic level
     */
    String getInstanceId();
}
