package thurber.java.exp;

import clojure.lang.Var;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.joda.time.Duration;
import thurber.java.Core;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;

import static java.util.Collections.singletonList;

/**
 * A data-driven variant of Beam's out-of-the-box
 * {@link org.apache.beam.sdk.transforms.windowing.Sessions} {@link WindowFn}.
 * <p>
 * A Clojure function ({@link Var}) is provided to the constructor; this function accepts
 * the processing element and returns either null or a {@link Duration} to indicate how
 * long before the window should be closed.
 * <p>
 * We call this the "fuse" function as it effectively starts a timer (from the current
 * processing element's timestamp) in event time after which the window will be closed.
 * Multiple elements may return different {@link Duration} values so that conceptually
 * multiple fuses are at play; the earliest expiring fuse wins and closes the window.
 * <p>
 * As long as the fuse function answers null for every element, the behavior of this windowing
 * will remain identical to {@link org.apache.beam.sdk.transforms.windowing.Sessions}
 * (the only other configuration provided to the constructor is a gap {@link Duration}
 * which has identical semantics to {@link org.apache.beam.sdk.transforms.windowing.Sessions}.)
 * <p>
 * The fuse function is generally expected to answer duration values less than gap duration,
 * though this is not required. Fuse functions that don't follow this general expectation may
 * see event gaps larger than `gapDuration` in the final window.
 *
 * @see <a href="https://beam.apache.org/documentation/patterns/custom-windows/">custom-windows</a>
 */
public final class TerminalWindowFn extends WindowFn<Object, TerminalWindow> {

    public static TerminalWindowFn withFuseAndGapDuration(Var fuseFn, Duration gapDuration) {
        return new TerminalWindowFn(fuseFn, gapDuration);
    }

    private final Var fuseFn;
    private final Duration gapDuration;

    private TerminalWindowFn(Var fuseFn, Duration gapDuration) {
        this.fuseFn = fuseFn;
        this.gapDuration = gapDuration;
    }

    @Override
    public Collection<TerminalWindow> assignWindows(AssignContext c) {
        @Nullable final Duration delay
            = (Duration) this.fuseFn.invoke(c.element());
        final boolean terminal = delay != null;
        return singletonList(
            new TerminalWindow(c.timestamp(),
                // safety: if fuse function answer 0, we bump to 1 as we never want the
                // current processing element itself to be outside of the window we
                // return.
                terminal ? c.timestamp().plus(Math.max(1, delay.getMillis()))
                    : c.timestamp().plus(gapDuration), terminal));
    }

    @Override
    public void mergeWindows(MergeContext c) throws Exception {
        TerminalWindowMerge.mergeWindows(c);
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
        return other instanceof TerminalWindowFn;
    }

    @Override
    public Coder<TerminalWindow> windowCoder() {
        return TerminalWindowCoder.INSTANCE;
    }

    @Override
    public boolean assignsToOneWindow() {
        return true;
    }

    @Override
    public WindowMappingFn<TerminalWindow> getDefaultWindowMappingFn() {
        throw new UnsupportedOperationException("no side input support yet");
    }

    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        Core.require_(this.fuseFn);
    }

}
