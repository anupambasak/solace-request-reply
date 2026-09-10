package org.cris.prs.messaging.solace.support;

import org.springframework.util.StringUtils;

/**
 * Builds the per-instance reply destinations. Appending the instance id as the last topic level is
 * what lets several replicas share one request topic while each receiving only its own replies.
 */
public final class ReplyDestinationResolver {

    private ReplyDestinationResolver() {
    }

    /**
     * Build this instance's reply topic.
     *
     * @param prefix           the configured base topic, e.g. {@code app/reply}
     * @param appendInstanceId whether to add the instance id as a further topic level
     * @param instanceId       this instance's id; ignored when blank
     * @return the base topic with the instance id appended, or the trimmed base topic when appending
     *         is off
     */
    public static String resolveTopic(String prefix, boolean appendInstanceId, String instanceId) {
        String base = trimTrailingSlash(prefix);
        if (!appendInstanceId || !StringUtils.hasText(instanceId)) {
            return base;
        }
        return base + "/" + HostnameInstanceIdProvider.sanitize(instanceId);
    }

    /**
     * Derive the reply endpoint's base name.
     *
     * <p>{@code /} is not valid in a Solace endpoint name, so a topic prefix is converted by replacing
     * it with {@code .}.</p>
     *
     * @param configuredQueue an explicit endpoint name; used as-is when it has text
     * @param topicPrefix     the reply topic prefix to derive a name from otherwise
     * @return the endpoint base name, e.g. {@code app.reply}
     */
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
