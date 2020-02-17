package thurber.java;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.Var;
import org.apache.beam.sdk.coders.Coder;

public class Core {

    static final IFn require = Clojure.var("clojure.core", "require");

    static final IFn concat = Clojure.var("clojure.core", "concat");

    static {
        require.invoke(Clojure.read("thurber"));
    }

    static Var nippy_ = (Var) Clojure.var("thurber", "nippy");

    @SuppressWarnings("unchecked")
    public static Coder<Object> nippy_deref_ = (Coder<Object>) nippy_.deref();

    // -- keywords --

    static final Keyword kw_th_coder_ = Keyword.intern("th", "coder");

    // -- thread locals --

    static final Var context_ = (Var) Clojure.var("thurber", "tl-context");
    static final Var proxy_args_ = (Var) Clojure.var("thurber", "tl-proxy-args");

    public static synchronized void require_(Var... vars) {
        for (Var var : vars)
            require.invoke(var.ns.name);
    }

    private Core() {}

}
