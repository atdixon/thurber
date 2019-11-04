package thurber.java;

import clojure.lang.Var;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class TCoder extends CustomCoder<Object> {

    private final Var encodefn, decodefn;

    public TCoder(Var encodefn, Var decodefn) {
        this.encodefn = encodefn;
        this.decodefn = decodefn;
    }

    @Override
    public void encode(Object val, OutputStream out) throws CoderException, IOException {
        this.encodefn.invoke(val, out);
    }

    @Override
    public Object decode(InputStream in) throws CoderException, IOException {
        return this.decodefn.invoke(in);
    }

    public void verifyDeterministic() {}

    public boolean consistentWithEquals() {
        return false;
    }

    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        Core.require_(this.encodefn);
        Core.require_(this.decodefn);
    }

}
