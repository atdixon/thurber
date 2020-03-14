package thurber.java;

import clojure.lang.IFn;
import clojure.lang.IObj;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.PersistentHashMap;
import clojure.lang.RT;
import clojure.lang.Var;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;

import java.io.IOException;
import java.util.Arrays;

import static java.lang.String.format;

public class TFn implements
    IObj,
    SerializableFunction<Object, Object>,
    SerializableBiFunction<Object, Object, Object> {

    public final Var fnVar;
    private transient IFn fn;
    public final Object[] partialArgs;
    // Note: meta doesn't carry through ser/de
    private transient IPersistentMap meta;

    public TFn(Var fnVar) {
        this(fnVar, new Object[]{});
    }

    public TFn(Var fnVar, Object[] partialArgs) {
        this(fnVar, partialArgs,
            fnVar.meta() == null ? PersistentHashMap.EMPTY
                : fnVar.meta());
    }

    public TFn(Var fnVar, Object[] partialArgs, IPersistentMap meta) {
        this.fnVar = fnVar;
        this.fn = (IFn) fnVar.deref();
        this.partialArgs = partialArgs;
        this.meta = meta;
    }

    // -- args --

    public TFn withoutPartialArgs() {
        return new TFn(this.fnVar, new Object[]{}, this.meta);
    }

    public TFn partial_(Object[] morePartialArgs) {
        final Object[] newPartialArgs =
            Arrays.copyOf(morePartialArgs, morePartialArgs.length + this.partialArgs.length);
        System.arraycopy(this.partialArgs, 0, newPartialArgs, morePartialArgs.length, this.partialArgs.length);
        return new TFn(this.fnVar, newPartialArgs, this.meta);
    }

    // -- meta --

    @Override public IObj withMeta(IPersistentMap meta) {
        return new TFn(fnVar, partialArgs, meta);
    }

    /** Note: metadata does not carry through ser/de; expectation is that metadata here is
     * consulted at pipeline construction time. */
    @Override public IPersistentMap meta() {
        return meta;
    }

    // -- invoke/apply --

    public Object invoke_() {
        if (partialArgs.length == 0)
            return fn.invoke();
        return invoke_(new Object[]{});
    }

    public Object invoke_(Object one) {
        if (partialArgs.length == 0)
            return fn.invoke(one);
        return invoke_(new Object[]{one});
    }

    public Object invoke_(Object one, Object two) {
        if (partialArgs.length == 0)
            return fn.invoke(one, two);
        return invoke_(new Object[]{one, two});
    }

    public Object invoke_(Object[] appendArgs) {
        if (partialArgs.length == 0) {
            if (appendArgs.length == 0)
                return fn.invoke();
            else if (appendArgs.length == 1)
                return fn.invoke(appendArgs[0]);
            else
                return fn.applyTo(RT.seq(appendArgs));
        } else if (appendArgs.length == 0) {
            if (partialArgs.length == 1)
                return fn.invoke(partialArgs[0]);
            return fn.applyTo(RT.seq(partialArgs));
        } else {
            return fn.applyTo((ISeq) Core.concat.invoke(RT.seq(partialArgs), RT.seq(appendArgs)));
        }
    }

    public Object apply_(ISeq appendArgs) {
        if (partialArgs.length == 0)
            return fn.applyTo(appendArgs);
        return fn.applyTo((ISeq) Core.concat.invoke(RT.seq(partialArgs), appendArgs));
    }

    @Override public Object apply(Object input) {
        return invoke_(input);
    }

    @Override public Object apply(Object o1, Object o2) {
        return invoke_(o1, o2);
    }

    // --

    @Override public String toString() {
        return format("TFn{%s}", fnVar);
    }

    // -- ser/de --

    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        Core.require_(fnVar);
        this.fn = (IFn) fnVar.deref();
    }

}
