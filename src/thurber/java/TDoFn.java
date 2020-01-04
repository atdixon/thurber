package thurber.java;

import clojure.lang.APersistentMap;
import clojure.lang.ISeq;
import clojure.lang.PersistentArrayMap;
import clojure.lang.RT;
import clojure.lang.Var;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

import javax.annotation.Nullable;
import java.util.Arrays;

public final class TDoFn extends DoFn<Object, Object> {

    private final Var fn;
    private final Object[] args;

    public TDoFn(Var fn) {
        this(fn, new Object[]{});
    }

    public TDoFn(Var fn, Object... args) {
        this.fn = fn;
        this.args = args;
    }

    @Setup
    public void setup() {
        Core.require_(fn);
    }

    @ProcessElement
    public void processElement(PipelineOptions options, ProcessContext context, BoundedWindow window) {
        execute(fn, args, options, context, window, null, null, null);
    }

    // --

    @SuppressWarnings("unchecked")
    private static final ThreadLocal<APersistentMap> context
        = (ThreadLocal<APersistentMap>) Core.context_.deref();

    static void execute(Var fn, Object[] args,
                        PipelineOptions options,
                        @Nullable ProcessContext processContext,
                        BoundedWindow window,
                        @Nullable ValueState<Object> state,
                        @Nullable Timer timer,
                        @Nullable OnTimerContext timerContext) {
        context.set(new PersistentArrayMap(new Object[]{
            Core.PO, options,
            Core.PC, processContext,
            Core.EW, window,
            Core.VS, state,
            Core.ET, timer,
            Core.TC, timerContext}));
        try {
            @Nullable final Object rv;
            if (processContext == null) {
                rv = args.length == 0
                    ? fn.invoke() : fn.applyTo(RT.seq(args));
            } else if (args.length == 0) {
                rv = fn.invoke(processContext.element());
            } else {
                final Object[] args_ = Arrays.copyOf(args, args.length + 1);
                args_[args.length] = processContext.element();
                rv = fn.applyTo(RT.seq(args_));
            }
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
        } finally {
            context.set(null);
        }
    }

}
