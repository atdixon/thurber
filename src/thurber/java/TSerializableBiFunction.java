package thurber.java;

import clojure.lang.ISeq;
import clojure.lang.RT;
import clojure.lang.Var;
import org.apache.beam.sdk.transforms.SerializableBiFunction;

import java.io.IOException;

public final class TSerializableBiFunction implements SerializableBiFunction {

    private final Var fn;
    private final Object[] args;

    public TSerializableBiFunction(Var fn, Object... args) {
        this.fn = fn;
        this.args = args;
    }

    @Override public Object apply(Object o1, Object o2) {
        return fn.applyTo((ISeq) Core.concat.invoke(RT.seq(args), RT.vector(o1, o2)));
    }

    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        Core.require_(this.fn);
    }

}
