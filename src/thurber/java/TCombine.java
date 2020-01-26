package thurber.java;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Var;
import org.apache.beam.sdk.transforms.Combine;

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

    // todo -- consult var metadata for these?? (how is the default determined btw?)
//    @Override
//    public Coder<Object> getAccumulatorCoder(CoderRegistry registry, Coder inputCoder) throws CannotProvideCoderException {
//        return (Coder<Object>) Core.nippy_.deref();
//    }
//
//    @Override
//    public Coder<Object> getDefaultOutputCoder(CoderRegistry registry, Coder inputCoder) throws CannotProvideCoderException {
//        return (Coder<Object>) Core.nippy_.deref();
//    }

    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        Core.require_(this.extractfVar, this.combinefVar, this.reducefVar);
        this.extractf = (IFn) extractfVar.deref();
        this.combinef = (IFn) combinefVar.deref();
        this.reducef = (IFn) reducefVar.deref();
    }

}
