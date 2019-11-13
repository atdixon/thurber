package thurber.java;

import clojure.lang.ISeq;
import clojure.lang.RT;
import clojure.lang.Var;
import org.apache.beam.sdk.transforms.SerializableFunction;

import java.io.IOException;

public final class TSerializableFunction implements SerializableFunction {

    private final Var fn;
    private final Object[] args;

    public TSerializableFunction(Var fn, Object... args) {
        this.fn = fn;
        this.args = args;
    }

    @Override public Object apply(Object input) {
        return fn.applyTo((ISeq) Core.concat.invoke(RT.seq(args), RT.vector(input)));
    }

    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        Core.require_(this.fn);
    }

}
