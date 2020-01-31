package thurber.java;

import clojure.lang.IFn;
import clojure.lang.Var;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

import javax.annotation.Nullable;

public final class TDoFn_Stateful extends DoFn<Object, Object> {

    private final Var fnVar;
    @Nullable private final Var timerFnVar;
    private transient IFn fn, timerFn;
    private final Object[] args, timerArgs;

    @StateId("val-state")
    private final StateSpec<ValueState<Object>> stateSpec =
        StateSpecs.value(Core.nippy_deref_);

    @StateId("bag-state")
    private final StateSpec<BagState<Object>> bagStateSpec =
        StateSpecs.bag(Core.nippy_deref_);

    @TimerId("timer")
    private final TimerSpec timerSpec
        = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    public TDoFn_Stateful(Var fnVar, Var timerFnVar, Object[] args, Object[] timerArgs) {
        this.fnVar = fnVar;
        this.timerFnVar = timerFnVar;
        this.args = args;
        this.timerArgs = timerArgs;
    }

    @Setup
    public void setup() {
        Core.require_(fnVar);
        this.fn = (IFn) fnVar.deref();
        if (this.timerFnVar != null) {
            Core.require_(timerFnVar);
            this.timerFn = (IFn) timerFnVar.deref();
        }
    }

    @ProcessElement
    public void processElement(PipelineOptions options, ProcessContext context, BoundedWindow window,
                               @StateId("val-state") ValueState<Object> state, @StateId("bag-state") BagState<Object> bagState,
                               @TimerId("timer") Timer timer) {
        TDoFn.execute(fn, args, options, context, window, state, bagState, timer, null, null);
    }

    @OnTimer("timer")
    public void onTimer(PipelineOptions options, OnTimerContext context, @TimerId("timer") Timer timer,
                        @StateId("val-state") ValueState<Object> state, @StateId("bag-state") BagState<Object> bagState) {
        TDoFn.execute(timerFn, timerArgs, options, null, null, state, bagState, timer, context, null);
    }

}
