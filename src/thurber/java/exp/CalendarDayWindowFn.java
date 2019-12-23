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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

import static java.util.Collections.singleton;

public class CalendarDayWindowFn extends NonMergingWindowFn<Object, IntervalWindow> {

    private static final DateTime DEFAULT_START_DATE = new DateTime(0, DateTimeZone.UTC);

    private final Var timezoneFn;

    public CalendarDayWindowFn(Var timezoneFn) {
        this.timezoneFn = timezoneFn;
    }

    @Override public Collection<IntervalWindow> assignWindows(AssignContext c) throws Exception {
        @Nonnull DateTimeZone tz = (DateTimeZone) timezoneFn.invoke(c.element());

        DateTime epoch = DEFAULT_START_DATE.withZoneRetainFields(tz);
        DateTime current = new DateTime(c.timestamp(), tz);

        int dayOffset = Days.daysBetween(epoch, current).getDays();

        DateTime begin = epoch.plusDays(dayOffset);
        DateTime end = begin.plusDays(1);

        return singleton(new IntervalWindow(begin.toInstant(), end.toInstant()));
    }

    @Override public boolean isCompatible(WindowFn<?, ?> other) {
        return other instanceof CalendarDayWindowFn;
    }

    @Override public Coder<IntervalWindow> windowCoder() {
        return IntervalWindow.getCoder();
    }

    @Override public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
        throw new UnsupportedOperationException("no side input support yet");
    }

    @Override public boolean assignsToOneWindow() {
        return true;
    }

}
