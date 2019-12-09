package thurber.java;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Var;

public class Core {

    private static final IFn require = Clojure.var("clojure.core", "require");

    static final IFn concat = Clojure.var("clojure.core", "concat");

    static {
        require.invoke(Clojure.read("thurber"));
    }

    static IFn apply__ = Clojure.var("thurber", "apply**");

    public static Var nippy_ = (Var) Clojure.var("thurber", "nippy");

    static IFn create_accumulator_ = Clojure.var("thurber", "create-accumulator");
    static IFn add_input_ = Clojure.var("thurber", "add-input");
    static IFn merge_accumulators_ = Clojure.var("thurber", "merge-accumulators");
    static IFn extract_output_ = Clojure.var("thurber", "extract-output");

    public static synchronized void require_(Var var) {
        require.invoke(var.ns.name);
    }

    private Core() {}

}
