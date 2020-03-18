package thurber.java;

import clojure.lang.RT;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.CombineWithContext;

import javax.annotation.Nullable;

public final class TCombine extends CombineWithContext.CombineFnWithContext<Object, Object, Object> {

    @SuppressWarnings("unchecked")
    private static final ThreadLocal<TFnContext> thContext
        = (ThreadLocal<TFnContext>) Core.context_.deref();

    private final TFn extractf, combinef, reducef;
    @Nullable private final Coder<Object> reducefCoder, extractfCoder;

    public TCombine(TFn extractf, TFn combinef, TFn reducef) {
        this.extractf = extractf;
        this.combinef = combinef;
        this.reducef = reducef;
        // Capture metadata on construction; serialization does not preserve metadata on
        //   function vars.
        //noinspection unchecked
        this.reducefCoder = (Coder<Object>) this.reducef.meta().valAt(Core.kw_th_coder_);
        //noinspection unchecked
        this.extractfCoder = (Coder<Object>) this.extractf.meta().valAt(Core.kw_th_coder_);
    }

    @Override public Object createAccumulator(CombineWithContext.Context context) {
        thContext.set(new TFnContext(context.getPipelineOptions(), context));
        try {
            return this.reducef.invoke_();
        } finally {
            thContext.set(null);
        }
    }

    @Override public Object addInput(Object acc, Object input, CombineWithContext.Context context) {
        thContext.set(new TFnContext(context.getPipelineOptions(), context));
        try {
            return this.reducef.invoke_(acc, input);
        } finally {
            thContext.set(null);
        }
    }

    @Override public Object mergeAccumulators(Iterable<Object> accs, CombineWithContext.Context context) {
        thContext.set(new TFnContext(context.getPipelineOptions(), context));
        try {
            return this.combinef.apply_(RT.seq(accs));
        } finally {
            thContext.set(null);
        }
    }

    @Override public Object extractOutput(Object acc, CombineWithContext.Context context) {
        thContext.set(new TFnContext(context.getPipelineOptions(), context));
        try {
            return this.extractf.invoke_(acc);
        } finally {
            thContext.set(null);
        }
    }

    @Override
    public Object defaultValue() {
        // In context where defaultValue is required the expectation is that extractf/reducef
        //  will not consult context state.
        thContext.set(TFnContext.EMPTY);
        try {
            return this.extractf.invoke_(this.reducef.invoke_());
        } finally {
            thContext.set(null);
        }
    }

    @SuppressWarnings("unchecked") @Override
    public Coder<Object> getAccumulatorCoder(CoderRegistry registry, Coder inputCoder) throws CannotProvideCoderException {
        if (this.reducefCoder != null)
            return this.reducefCoder;
        return super.getAccumulatorCoder(registry, inputCoder);
    }

    @SuppressWarnings("unchecked") @Override
    public Coder<Object> getDefaultOutputCoder(CoderRegistry registry, Coder inputCoder) throws CannotProvideCoderException {
        if (this.extractfCoder != null)
            return this.extractfCoder;
        return super.getDefaultOutputCoder(registry, inputCoder);
    }

}
