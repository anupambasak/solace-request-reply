package cris.prs.messaging.solace.support;

import org.springframework.util.StringUtils;

/**
 * Builds the per-instance reply destinations. Appending the instance id as the last topic level is
 * what lets several replicas share one request topic while each receiving only its own replies.
 */
public final class ReplyDestinationResolver {

    private ReplyDestinationResolver() {
    }

    /** {@code bkgRep/trn} + {@code client-7d9f} &rarr; {@code bkgRep/trn/client-7d9f}. */
    public static String resolveTopic(String prefix, boolean appendInstanceId, String instanceId) {
        String base = trimTrailingSlash(prefix);
        if (!appendInstanceId || !StringUtils.hasText(instanceId)) {
            return base;
        }
        return base + "/" + HostnameInstanceIdProvider.sanitize(instanceId);
    }

    /** {@code bkgRep/trn} &rarr; {@code bkgRep.trn}, a legal Solace endpoint name. */
    public static String resolveQueueBaseName(String configuredQueue, String topicPrefix) {
        if (StringUtils.hasText(configuredQueue)) {
            return configuredQueue;
        }
        return trimTrailingSlash(topicPrefix).replace('/', '.');
    }

    private static String trimTrailingSlash(String value) {
        String trimmed = value.trim();
        return trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
    }
}
