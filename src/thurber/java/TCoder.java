package thurber.java;

import clojure.lang.Var;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class TCoder extends CustomCoder<Object> {

    private final Var delegate;

    public TCoder(Var delegate) {
        this.delegate = delegate;
    }

    @Override
    public void encode(Object val, OutputStream out) throws CoderException, IOException {
        ((CustomCoder<Object>) this.delegate.deref())
            .encode(val, out);
    }

    @Override
    public Object decode(InputStream in) throws CoderException, IOException {
        return ((CustomCoder<Object>) this.delegate.deref())
            .decode(in);
    }

    public void verifyDeterministic() {}

    public boolean consistentWithEquals() {
        return true;
    }

    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        Core.require_(this.delegate);
    }

}
