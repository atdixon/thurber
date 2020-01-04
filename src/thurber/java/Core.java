package thurber.java;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Var;
import org.apache.beam.sdk.coders.Coder;

public class Core {

    private static final IFn require = Clojure.var("clojure.core", "require");

    static final IFn concat = Clojure.var("clojure.core", "concat");

    static {
        require.invoke(Clojure.read("thurber"));
    }

    static IFn apply__ = Clojure.var("thurber", "apply**");
    static IFn apply_timer__ = Clojure.var("thurber", "apply-timer**");

    static Var nippy_ = (Var) Clojure.var("thurber", "nippy");

    @SuppressWarnings("unchecked")
    public static Coder<Object> nippy_deref_ = (Coder<Object>) nippy_.deref();

    static IFn create_accumulator_ = Clojure.var("thurber", "create-accumulator");
    static IFn add_input_ = Clojure.var("thurber", "add-input");
    static IFn merge_accumulators_ = Clojure.var("thurber", "merge-accumulators");
    static IFn extract_output_ = Clojure.var("thurber", "extract-output");

    // -- dynamic --

    static final Var PO = (Var) Clojure.var("thurber", "*pipeline-options*");
    static final Var PC = (Var) Clojure.var("thurber", "*process-context*");
    static final Var EW = (Var) Clojure.var("thurber", "*element-window*");
    static final Var VS = (Var) Clojure.var("thurber", "*value-state*");
    static final Var ET = (Var) Clojure.var("thurber", "*event-timer*");
    static final Var TC = (Var) Clojure.var("thurber", "*timer-context*");

    public static synchronized void require_(Var var) {
        require.invoke(var.ns.name);
    }

    private Core() {}

}
