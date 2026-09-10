package org.cris.prs.messaging.solace.support;

/**
 * Matches a published topic against a Solace subscription pattern.
 *
 * <p>The broker does this matching when it decides which endpoints a message is spooled to. This
 * class does it again, client-side, so one container bound to a queue with several subscriptions can
 * work out <em>which</em> subscription a delivered message matched, and route it to the right
 * listener method. See {@code TopicDispatchingSolaceListener}.</p>
 *
 * <h2>Solace wildcard rules</h2>
 * <p>Topics are {@code /}-separated levels. Two wildcards:</p>
 * <table border="1">
 *   <caption>Wildcards</caption>
 *   <tr><th>Pattern</th><th>Matches</th><th>Does not match</th></tr>
 *   <tr><td>{@code orders/*}</td><td>{@code orders/created}</td>
 *       <td>{@code orders}, {@code orders/eu/created}</td></tr>
 *   <tr><td>{@code orders/&gt;}</td><td>{@code orders/created}, {@code orders/eu/created}</td>
 *       <td>{@code orders}</td></tr>
 *   <tr><td>{@code orders/cr*}</td><td>{@code orders/created}, {@code orders/cr}</td>
 *       <td>{@code orders/cancelled}</td></tr>
 *   <tr><td>{@code &gt;}</td><td>everything</td><td>&mdash;</td></tr>
 * </table>
 *
 * <p>The two are genuinely different and the difference matters here: {@code *} is exactly one level,
 * {@code >} is one or more trailing levels and is only meaningful as the final character. A
 * {@code *} may carry a prefix within its level ({@code cr*}) but not a suffix.</p>
 */
public final class SolaceTopicMatcher {

    private SolaceTopicMatcher() {
    }

    /**
     * Whether a topic matches a subscription pattern.
     *
     * @param pattern the subscription, possibly containing {@code *} and a trailing {@code >}
     * @param topic   the published topic
     * @return {@code true} when the broker would consider them a match
     */
    public static boolean matches(String pattern, String topic) {
        if (pattern == null || topic == null) {
            return false;
        }
        if (pattern.equals(topic)) {
            return true;
        }
        return matches(split(pattern), split(topic));
    }

    /**
     * Match level by level.
     *
     * @param patternLevels the subscription's levels
     * @param topicLevels   the topic's levels
     * @return whether they match
     */
    private static boolean matches(String[] patternLevels, String[] topicLevels) {
        int index = 0;
        while (index < patternLevels.length) {
            String level = patternLevels[index];

            if (">".equals(level)) {
                // Only meaningful as the last level, and it needs at least one level to consume:
                // "orders/>" does not match the bare topic "orders".
                return index == patternLevels.length - 1 && topicLevels.length > index;
            }
            if (index >= topicLevels.length) {
                return false;
            }
            if (!levelMatches(level, topicLevels[index])) {
                return false;
            }
            index++;
        }
        // Every pattern level matched; the topic must not have levels left over.
        return topicLevels.length == patternLevels.length;
    }

    /**
     * Match one level, honouring {@code *} and a {@code prefix*} within the level.
     *
     * @param patternLevel one level of the subscription
     * @param topicLevel   the corresponding level of the topic
     * @return whether that level matches
     */
    private static boolean levelMatches(String patternLevel, String topicLevel) {
        if ("*".equals(patternLevel)) {
            return true;
        }
        if (patternLevel.endsWith("*")) {
            return topicLevel.startsWith(patternLevel.substring(0, patternLevel.length() - 1));
        }
        return patternLevel.equals(topicLevel);
    }

    /**
     * Split a topic into levels.
     *
     * @param value the topic or subscription
     * @return its levels
     */
    private static String[] split(String value) {
        return value.split("/", -1);
    }
}
