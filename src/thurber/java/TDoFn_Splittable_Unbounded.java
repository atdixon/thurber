package thurber.java;

import clojure.lang.IFn;
import clojure.lang.Var;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;

@DoFn.UnboundedPerElement
public final class TDoFn_Splittable_Unbounded extends DoFn<Object, Object> {

    private final Var fnVar, initFnVar, trackerFnVar;
    private transient IFn fn, initFn, trackerFn;
    private final Object[] args;

    public TDoFn_Splittable_Unbounded(Var fnVar, Var initFnVar, Var trackerFn) {
        this(fnVar, initFnVar, trackerFn, new Object[]{});
    }

    public TDoFn_Splittable_Unbounded(Var fnVar, Var initFnVar, Var trackerFnVar, Object[] args) {
        this.fnVar = fnVar;
        this.initFnVar = initFnVar;
        this.trackerFnVar = trackerFnVar;
        this.args = args;
    }

    @Setup
    public void setup() {
        Core.require_(fnVar, initFnVar, trackerFnVar);
        this.fn = (IFn) fnVar.deref();
        this.initFn = (IFn) initFnVar.deref();
        this.trackerFn = (IFn) trackerFnVar.deref();
    }

    @ProcessElement
    public ProcessContinuation process(PipelineOptions options, ProcessContext context,
                                       RestrictionTracker<Object, Object> tracker) {
        return (ProcessContinuation)
            TDoFn.execute(fn, args, options, context, null, null, null, null, null, tracker);
    }

    @GetInitialRestriction
    public Object getInitialRestriction(Object element) {
        return initFn.invoke(element);
    }

    @NewTracker
    public <R extends RestrictionTracker<Object, ?>> R getNewTracker(Object restriction) {
        return (R) trackerFn.invoke(restriction);
    }

    // todo custom restriction coder
    @GetRestrictionCoder
    public Coder<Object> getRestrictionCoder() {
        return Core.nippy_deref_;
    }

}
