package thurber.java;

import clojure.lang.RT;
import clojure.lang.Var;
import org.apache.beam.sdk.transforms.Combine;

import java.io.IOException;

public final class TCombine extends Combine.CombineFn<Object, Object, Object> {

    private final Var cfn;

    public TCombine(Var cfn) {
        this.cfn = cfn;
    }

    @Override public Object createAccumulator() {
        return Core.create_accumulator_.invoke(this.cfn.deref());
    }

    @Override public Object addInput(Object acc, Object input) {
        return Core.add_input_.invoke(this.cfn.deref(), acc, input);
    }

    @Override public Object mergeAccumulators(Iterable<Object> accs) {
        return Core.merge_accumulators_.invoke(this.cfn.deref(), accs);
    }

    @Override public Object extractOutput(Object acc) {
        return Core.extract_output_.invoke(this.cfn.deref(), acc);
    }

    // todo
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
        Core.require_(this.cfn);
    }

}
