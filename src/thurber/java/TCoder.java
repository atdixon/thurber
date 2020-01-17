package thurber.java;

import clojure.lang.Var;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class TCoder extends CustomCoder<Object> {

    private final Var delegate;
    private transient CustomCoder<Object> coder;

    public TCoder(Var delegate) {
        this.delegate = delegate;
        //noinspection unchecked
        this.coder = (CustomCoder<Object>) delegate.deref();
    }

    @Override
    public void encode(Object val, OutputStream out) throws CoderException, IOException {
        coder.encode(val, out);
    }

    @Override
    public Object decode(InputStream in) throws CoderException, IOException {
        return coder.decode(in);
    }

    public void verifyDeterministic() {}

    public boolean consistentWithEquals() {
        return true;
    }

    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        Core.require_(this.delegate);
        //noinspection unchecked
        this.coder = (CustomCoder<Object>) delegate.deref();
    }

}
