package thurber.java;

import clojure.lang.ISeq;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

import javax.annotation.Nullable;

import static java.lang.String.format;

public final class TDoFn extends DoFn<Object, Object> {

    private final TFn tfn;

    public TDoFn(TFn tfn) {
        this.tfn = tfn;
    }

    @ProcessElement
    public void processElement(PipelineOptions options, ProcessContext context, BoundedWindow window) {
        execute(this.tfn, options, context, window, null, null, null, null, null);
    }

    // --

    @SuppressWarnings("unchecked")
    private static final ThreadLocal<TFnContext> thContext
        = (ThreadLocal<TFnContext>) Core.context_.deref();

    static Object execute(TFn tfn,
                          PipelineOptions options,
                          @Nullable ProcessContext processContext,
                          BoundedWindow window,
                          @Nullable ValueState<Object> state,
                          @Nullable BagState<Object> bagState,
                          @Nullable Timer timer,
                          @Nullable OnTimerContext timerContext,
                          @Nullable RestrictionTracker<Object, Object> restrictionTracker) {
        thContext.set(new TFnContext(
            options, processContext, window, state, bagState, timer, timerContext, null, restrictionTracker));
        try {
            @Nullable final Object rv;
            if (processContext == null) {
                rv = tfn.invoke_();
            } else {
                rv = tfn.invoke_(processContext.element());
            }
            if (rv instanceof ProcessContinuation)
                return rv;
            if (rv != null) {
                WindowedContext useContext = processContext != null
                    ? processContext : timerContext;
                if (useContext == null)
                    throw new IllegalStateException();
                if (rv instanceof ISeq) {
                    ISeq seq = (ISeq) rv;
                    for (; seq.first() != null; seq = seq.more()) {
                        useContext.output(seq.first());
                    }
                } else {
                    useContext.output(rv);
                }
            }
        } catch (Exception t) {
            throw new RuntimeException(
                format("Execution failure from: %s", tfn), t);
        } finally {
            thContext.set(null);
        }
        return null;
    }

}
