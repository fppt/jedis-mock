package com.github.fppt.jedismock.operations.strings;

/**
 * The {@code EX | PX | EXAT | PXAT} argument that SET and GETEX both take.
 * <p>
 * Real Redis validates it in one place, {@code getExpireMillisecondsOrReply},
 * and the easy part to get wrong is the last step: only a <em>relative</em>
 * expiration has the current time added to it, so {@code EXAT} accepts
 * deadlines that the very same number would overflow as an {@code EX}.
 */
final class ExpirationArgument {

    private ExpirationArgument() {
    }

    /**
     * The argument in milliseconds — a duration for the relative options, an
     * absolute deadline for {@code EXAT} and {@code PXAT}.
     *
     * @param command the name to quote in an invalid-expire-time reply
     * @throws IllegalArgumentException carrying the reply Redis would send
     */
    static long millis(String value, boolean seconds, boolean absolute, long now, String command) {
        long parsed;
        try {
            parsed = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("ERR value is not an integer or out of range");
        }
        //A non-positive expiration is out of range for every unit, absolute
        //ones included, and seconds are rejected once they overflow milliseconds
        if (parsed <= 0 || seconds && parsed > Long.MAX_VALUE / 1000) {
            throw invalidExpireTime(command);
        }
        long millis = seconds ? parsed * 1000 : parsed;
        //Only a relative expiration is later offset by the current time, so
        //only it can overflow; an absolute deadline is already the answer
        if (!absolute && millis >= Long.MAX_VALUE - now) {
            throw invalidExpireTime(command);
        }
        return millis;
    }

    static IllegalArgumentException invalidExpireTime(String command) {
        return new IllegalArgumentException(
                String.format("ERR invalid expire time in '%s' command", command));
    }
}
