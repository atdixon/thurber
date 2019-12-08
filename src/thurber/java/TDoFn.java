package thurber.java;

import clojure.lang.Var;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

public final class TDoFn extends org.apache.beam.sdk.transforms.DoFn<Object, Object> {

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
        Core.apply__.invoke(fn, options, context, window, args);
    }

}
