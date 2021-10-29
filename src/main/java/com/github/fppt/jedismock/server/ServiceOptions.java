package com.github.fppt.jedismock.server;

/**
 * Created by Xiaolu on 2015/4/22.
 */
public class ServiceOptions {
    private final int autoCloseOn;

    private ServiceOptions(
            int autoCloseOn) {
        this.autoCloseOn = autoCloseOn;
    }

    public int autoCloseOn() {
        return autoCloseOn;
    }

    @Override
    public String toString() {
        return "ServiceOptions{"
                + "autoCloseOn=" + autoCloseOn
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof ServiceOptions) {
            ServiceOptions that = (ServiceOptions) o;
            return (this.autoCloseOn == that.autoCloseOn());
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h$ = 1;
        h$ *= 1000003;
        h$ ^= autoCloseOn;
        return h$;
    }

    public static ServiceOptions defaultOptions() {
        return new ServiceOptions(0);
    }

    public static ServiceOptions create(int autoCloseOn) {
        return new ServiceOptions(autoCloseOn);
    }
}
