package thurber.java;

import clojure.java.api.Clojure;
import clojure.lang.PersistentArrayMap;
import clojure.lang.Var;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;

public class TProxy {

    /** Serializable MethodHandler. */
    private static final class MethodHandlerImpl implements MethodHandler, Serializable {
        private final Var proxyVar;
        private final Object[] proxyArgs;

        private MethodHandlerImpl(Var proxyVar, Object[] args) {
            this.proxyVar = proxyVar;
            this.proxyArgs = args;
        }

        @Override public Object invoke(Object self, Method thisMethod, Method proceed, Object[] args) throws Throwable {
            Var.pushThreadBindings(PersistentArrayMap.create(
                Collections.singletonMap((Var) Clojure.var("thurber", "*proxy-args*"), proxyArgs)));
            try {
                return thisMethod.invoke(proxyVar.deref(), args);
            } finally {
                Var.popThreadBindings();
            }
        }

        private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
            stream.defaultReadObject();
            Core.require_(proxyVar);
        }
    }

    public static Object create(Var proxyVar, @Nullable String signature, Object... args) {
        final Class<?> originalProxyClass
            = proxyVar.deref().getClass();

        final ProxyFactory f = new ProxyFactory();
        f.setSuperclass(originalProxyClass.getSuperclass());
        f.setInterfaces(add_(originalProxyClass.getInterfaces(), Serializable.class));
        f.setGenericSignature(signature);
        try {
            return f.create(new Class[0], new Object[0], new MethodHandlerImpl(proxyVar, args));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Class<?>[] add_(Class<?>[] arr, Class<?> item) {
        for (Class<?> i : arr)
            if (i == item)
                return arr;
        final Class[] rv = Arrays.copyOf(arr, arr.length + 1);
        rv[rv.length - 1] = item;
        return rv;
    }

}
