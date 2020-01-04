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

    static IFn create_accumulator_ = Clojure.var("thurber", "create-accumulator");
    static IFn add_input_ = Clojure.var("thurber", "add-input");
    static IFn merge_accumulators_ = Clojure.var("thurber", "merge-accumulators");
    static IFn extract_output_ = Clojure.var("thurber", "extract-output");

    // -- thread locals --

    static final Var context_ = (Var) Clojure.var("thurber", "tl-context");
    static final Var proxy_args_ = (Var) Clojure.var("thurber", "tl-proxy-args");

    static final Keyword PO = Keyword.intern("pipeline-options");
    static final Keyword PC = Keyword.intern("process-context");
    static final Keyword EW = Keyword.intern("element-window");
    static final Keyword VS = Keyword.intern("value-state");
    static final Keyword ET = Keyword.intern("event-timer");
    static final Keyword TC = Keyword.intern("timer-context");

    public static synchronized void require_(Var var) {
        require.invoke(var.ns.name);
    }

    private Core() {}

}
