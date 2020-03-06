package thurber.java;

import clojure.lang.RT;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;

import javax.annotation.Nullable;

public final class TCombine extends Combine.CombineFn<Object, Object, Object> {

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

    @Override public Object createAccumulator() {
        return this.reducef.invoke_();
    }

    @Override public Object addInput(Object acc, Object input) {
        return this.reducef.invoke_(acc, input);
    }

    @Override public Object mergeAccumulators(Iterable<Object> accs) {
        return this.combinef.apply_(RT.seq(accs));
    }

    @Override public Object extractOutput(Object acc) {
        return this.extractf.invoke_(acc);
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
