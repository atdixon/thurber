package thurber.java;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

public final class TFnContext {

    public static final TFnContext EMPTY = new TFnContext();

    public final PipelineOptions pipelineOptions;
    public final DoFn<?, ?>.ProcessContext processContext;
    public final BoundedWindow elementWindow;
    public final ValueState<Object> valueState;
    public final BagState<Object> bagState;
    public final Timer eventTimer;
    public final DoFn<?, ?>.OnTimerContext timerContext;
    public final CombineWithContext.Context combineContext;
    public final RestrictionTracker<Object, Object> restrictionTracker;

    private TFnContext() {
        this(null, null, null, null, null, null, null, null, null);
    }

    public TFnContext(PipelineOptions pipelineOptions, CombineWithContext.Context combineContext) {
        this(pipelineOptions, null, null, null, null, null, null, combineContext, null);
    }

    public TFnContext(PipelineOptions pipelineOptions,
                      DoFn<?, ?>.ProcessContext processContext,
                      BoundedWindow elementWindow,
                      ValueState<Object> valueState,
                      BagState<Object> bagState,
                      Timer eventTimer,
                      DoFn<?, ?>.OnTimerContext timerContext,
                      CombineWithContext.Context combineContext,
                      RestrictionTracker<Object, Object> restrictionTracker) {
        this.pipelineOptions = pipelineOptions;
        this.processContext = processContext;
        this.elementWindow = elementWindow;
        this.valueState = valueState;
        this.bagState = bagState;
        this.eventTimer = eventTimer;
        this.timerContext = timerContext;
        this.combineContext = combineContext;
        this.restrictionTracker = restrictionTracker;
    }

}
