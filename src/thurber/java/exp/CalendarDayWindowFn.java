package thurber.java.exp;

import clojure.lang.Var;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import thurber.java.Core;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;

import static java.util.Collections.singleton;

/**
 * A data-driven variant of Beam's out-of-the-box
 * {@link org.apache.beam.sdk.transforms.windowing.CalendarWindows.DaysWindows} {@link WindowFn}.
 * <p>
 * A Clojure function ({@link Var}) is provided to the constructor; this function accepts
 * the processing element and returns a non-null {@link DateTimeZone} indicating the timezone
 * of the element.
 * <p>
 * The timezone is used in conjunction with the element's {@link AssignContext#timestamp()} to place
 * the element in a window corresponding precisely corresponding to the whole day containing the
 * timestamp in the timezone.
 * <p>
 * Elements are placed in a unique, single window and windows never are merged.
 *
 * @see org.apache.beam.sdk.transforms.windowing.Sessions
 */
public class CalendarDayWindowFn extends NonMergingWindowFn<Object, IntervalWindow> {

    public static CalendarDayWindowFn forTimezoneFn(Var timezoneFn) {
        return new CalendarDayWindowFn(timezoneFn);
    }

    private static final DateTime DEFAULT_START_DATE = new DateTime(0, DateTimeZone.UTC);

    private final Var timezoneFn;

    private CalendarDayWindowFn(Var timezoneFn) {
        this.timezoneFn = timezoneFn;
    }

    @Override
    public Collection<IntervalWindow> assignWindows(AssignContext c) throws Exception {
        @Nonnull final DateTimeZone tz = (DateTimeZone) timezoneFn.invoke(c.element());

        final DateTime epoch = DEFAULT_START_DATE.withZoneRetainFields(tz);
        final DateTime current = new DateTime(c.timestamp(), tz);

        int dayOffset = Days.daysBetween(epoch, current).getDays();

        final DateTime begin = epoch.plusDays(dayOffset);
        final DateTime end = begin.plusDays(1);

        return singleton(new IntervalWindow(begin.toInstant(), end.toInstant()));
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
        return other instanceof CalendarDayWindowFn;
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
        return IntervalWindow.getCoder();
    }

    @Override
    public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
        throw new UnsupportedOperationException("no side input support yet");
    }

    @Override
    public boolean assignsToOneWindow() {
        return true;
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof CalendarDayWindowFn)) {
            return false;
        }
        CalendarDayWindowFn other = (CalendarDayWindowFn) object;
        return timezoneFn.equals(other.timezoneFn);
    }

    @Override
    public int hashCode() {
        return timezoneFn.hashCode();
    }

    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        Core.require_(this.timezoneFn);
    }

}
