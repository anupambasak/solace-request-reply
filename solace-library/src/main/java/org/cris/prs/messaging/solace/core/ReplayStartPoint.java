package org.cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.ReplayStartLocation;

import java.time.Instant;
import java.util.Date;
import java.util.Objects;

/**
 * Where a replay starts.
 *
 * <p>Replay asks the broker to re-deliver messages it has already spooled, from the start of its
 * replay log or from a point in time. It turns the broker into a short-term event store: rebuild a
 * projection after a bug, or bring a new service online with history rather than only new
 * events.</p>
 *
 * <p>Three things to understand before using it, because none of them is obvious:</p>
 * <ul>
 *   <li><b>Replay is a property of the flow, not of the message.</b> Starting a replay rebinds the
 *       flow; the endpoint's existing messages are re-delivered from the chosen point and live
 *       delivery resumes afterwards.</li>
 *   <li><b>It affects the whole endpoint.</b> On a queue shared by several instances, a replay
 *       started by one of them re-delivers to <em>all</em> consumers of that endpoint. It is not a
 *       private read.</li>
 *   <li><b>The broker must have replay enabled</b> for the Message VPN, with a replay log large
 *       enough to still hold the period asked for. A replay the broker cannot satisfy fails the
 *       flow, arriving as a {@code DOWN} flow event rather than as an exception from the call that
 *       requested it.</li>
 * </ul>
 *
 * <p>Immutable, and comparable by value, so a container can tell whether a replay request actually
 * changes anything.</p>
 */
public final class ReplayStartPoint {

    private static final ReplayStartPoint BEGINNING = new ReplayStartPoint(null);

    /** The instant to replay from, or {@code null} for the beginning of the replay log. */
    private final Instant from;

    private ReplayStartPoint(Instant from) {
        this.from = from;
    }

    /**
     * Replay everything the broker still holds.
     *
     * @return a start point at the beginning of the replay log
     */
    public static ReplayStartPoint beginning() {
        return BEGINNING;
    }

    /**
     * Replay from a point in time.
     *
     * @param from the instant to replay from; messages spooled before it are skipped
     * @return the start point
     */
    public static ReplayStartPoint from(Instant from) {
        Objects.requireNonNull(from, "'from' must not be null");
        return new ReplayStartPoint(from);
    }

    /**
     * Parse a configured value.
     *
     * <p>Accepts {@code BEGINNING} (any case) or an ISO-8601 instant such as
     * {@code 2026-08-23T10:15:30Z}, which is what makes the annotation attribute and the YAML
     * property usable.</p>
     *
     * @param value the configured value; {@code null} or blank means no replay
     * @return the start point, or {@code null} when nothing was configured
     * @throws IllegalArgumentException if the value is neither {@code BEGINNING} nor a valid
     *                                  ISO-8601 instant
     */
    public static ReplayStartPoint parse(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String trimmed = value.trim();
        if ("BEGINNING".equalsIgnoreCase(trimmed)) {
            return beginning();
        }
        try {
            return from(Instant.parse(trimmed));
        }
        catch (RuntimeException ex) {
            throw new IllegalArgumentException("Invalid replay start point '" + value
                    + "': expected BEGINNING or an ISO-8601 instant such as 2026-08-23T10:15:30Z", ex);
        }
    }

    /**
     * Whether this replays the whole log rather than from a timestamp.
     *
     * @return {@code true} for {@link #beginning()}
     */
    public boolean isBeginning() {
        return this.from == null;
    }

    /**
     * The instant this replays from.
     *
     * @return the instant, or {@code null} for {@link #beginning()}
     */
    public Instant getFrom() {
        return this.from;
    }

    /**
     * Convert to the JCSMP representation.
     *
     * @return the JCSMP replay start location
     */
    public ReplayStartLocation toReplayStartLocation() {
        return this.from == null
                ? JCSMPFactory.onlyInstance().createReplayStartLocationBeginning()
                : JCSMPFactory.onlyInstance().createReplayStartLocationDate(Date.from(this.from));
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object other) {
        return other instanceof ReplayStartPoint point && Objects.equals(this.from, point.from);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hashCode(this.from);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return this.from == null ? "BEGINNING" : this.from.toString();
    }
}
