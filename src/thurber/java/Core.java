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

    static Var nippy_ = (Var) Clojure.var("thurber", "nippy");

    public static synchronized void require_(Var var) {
        require.invoke(var.ns.name);
    }

    private Core() {}

}
