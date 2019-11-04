package thurber.java;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Var;

public class Core {

    private static final IFn require = Clojure.var("clojure.core", "require");

    static {
        require.invoke(Clojure.read("thurber.apply-dofn"));
        require.invoke(Clojure.read("thurber.coder"));
        require.invoke(Clojure.read("thurber"));
    }

    static IFn apply__ = Clojure.var("thurber.apply-dofn", "apply**");

    static Var nippy_coder_ = (Var) Clojure.var("thurber.coder", "nippy");

    static synchronized void require_(Var fn) {
        require.invoke(fn.ns.name);
    }

    private Core() {
    }

}
