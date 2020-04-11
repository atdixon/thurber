package thurber.java;

import clojure.lang.IFn;
import clojure.lang.Var;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;

@DoFn.UnboundedPerElement
public final class TDoFn_Splittable_Unbounded extends DoFn<Object, Object> {

    private final TFn tfn, initTfn, trackerTfn;

    public TDoFn_Splittable_Unbounded(TFn tfn, TFn initTfn, TFn trackerTfn) {
        this.tfn = tfn;
        this.initTfn = initTfn;
        this.trackerTfn = trackerTfn;
    }

    @ProcessElement
    public ProcessContinuation process(PipelineOptions options, ProcessContext context,
                                       RestrictionTracker<Object, Object> tracker) {
        return (ProcessContinuation)
            TDoFn.execute(tfn, options, context, null, null, null, null, null, tracker);
    }

    @GetInitialRestriction
    public Object getInitialRestriction(@Element Object element) {
        return initTfn.invoke_(element);
    }

    @NewTracker
    public <R extends RestrictionTracker<Object, ?>> R getNewTracker(/*@Restriction*/ Object restriction) {
        return (R) trackerTfn.invoke_(restriction);
    }

    // todo custom restriction coder
    @GetRestrictionCoder
    public Coder<Object> getRestrictionCoder() {
        return Core.nippy_deref_;
    }

}
