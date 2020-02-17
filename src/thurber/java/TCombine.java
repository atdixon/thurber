package thurber.java;

import clojure.lang.IFn;
import clojure.lang.IMeta;
import clojure.lang.RT;
import clojure.lang.Var;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;

import javax.annotation.Nullable;
import java.io.IOException;

public final class TCombine extends Combine.CombineFn<Object, Object, Object> {

    private final Var extractfVar, combinefVar, reducefVar;
    private transient IFn extractf, combinef, reducef;

    public TCombine(Var extractfVar, Var combinefVar, Var reducefVar) {
        this.extractfVar = extractfVar;
        this.combinefVar = combinefVar;
        this.reducefVar = reducefVar;
        this.extractf = (IFn) extractfVar.deref();
        this.combinef = (IFn) combinefVar.deref();
        this.reducef = (IFn) reducefVar.deref();
    }

    @Override public Object createAccumulator() {
        return this.reducef.invoke();
    }

    @Override public Object addInput(Object acc, Object input) {
        return this.reducef.invoke(acc, input);
    }

    @Override public Object mergeAccumulators(Iterable<Object> accs) {
        return this.combinef.applyTo(RT.seq(accs));
    }

    @Override public Object extractOutput(Object acc) {
        return this.extractf.invoke(acc);
    }

    @SuppressWarnings("unchecked") @Override
    public Coder<Object> getAccumulatorCoder(CoderRegistry registry, Coder inputCoder) throws CannotProvideCoderException {
        Object reducefCoder = getMeta_(this.reducefVar, Core.kw_th_coder_);
        if (reducefCoder != null)
            return (Coder<Object>) reducefCoder;
        return super.getAccumulatorCoder(registry, inputCoder);
    }

    @SuppressWarnings("unchecked") @Override
    public Coder<Object> getDefaultOutputCoder(CoderRegistry registry, Coder inputCoder) throws CannotProvideCoderException {
        Object extractfCoder = getMeta_(this.extractfVar, Core.kw_th_coder_);
        if (extractfCoder != null)
            return (Coder<Object>) extractfCoder;
        return super.getDefaultOutputCoder(registry, inputCoder);
    }

    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        Core.require_(this.extractfVar, this.combinefVar, this.reducefVar);
        this.extractf = (IFn) extractfVar.deref();
        this.combinef = (IFn) combinefVar.deref();
        this.reducef = (IFn) reducefVar.deref();
    }

    @Nullable
    private static Object getMeta_(IMeta meta, Object key) {
        if (meta.meta() != null)
            return meta.meta().valAt(key);
        return null;
    }

}
