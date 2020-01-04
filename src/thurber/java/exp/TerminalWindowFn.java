package thurber.java.exp;

import clojure.lang.Var;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.joda.time.Duration;
import thurber.java.Core;

import java.io.IOException;
import java.util.Collection;

import static java.util.Collections.singletonList;

public final class TerminalWindowFn extends WindowFn<Object, TerminalWindow> {

    private final Var fuseFn;
    private final Duration gap;

    public TerminalWindowFn(Var fuseFn, Duration gap) {
        this.fuseFn = fuseFn;
        this.gap = gap;
    }

    @Override
    public Collection<TerminalWindow> assignWindows(AssignContext c) throws Exception {
        Duration delay
            = (Duration) this.fuseFn.invoke(c.element());
        boolean terminal = delay != null;
        return singletonList(
            new TerminalWindow(c.timestamp(),
                terminal ? c.timestamp().plus(Math.max(1, delay.getMillis()))
                    : c.timestamp().plus(gap), terminal));
    }

    @Override
    public void mergeWindows(MergeContext c) throws Exception {
        TerminalWindowMerge.mergeWindows(c);
    }

    @Override
    public boolean assignsToOneWindow() {
        return true;
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
    public WindowMappingFn<TerminalWindow> getDefaultWindowMappingFn() {
        throw new UnsupportedOperationException("no side input support yet");
    }

    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        Core.require_(this.fuseFn);
    }

}
