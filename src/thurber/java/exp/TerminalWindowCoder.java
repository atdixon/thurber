package thurber.java.exp;

import org.apache.beam.sdk.coders.*;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

public final class TerminalWindowCoder extends Coder<TerminalWindow> {

    static final TerminalWindowCoder INSTANCE = new TerminalWindowCoder();

    private static final Coder<Instant> instantCoder = NullableCoder.of(InstantCoder.of());
    private static final Coder<Boolean> booleanCoder = BooleanCoder.of();

    @Override public void encode(TerminalWindow value, OutputStream outStream) throws CoderException, IOException {
        instantCoder.encode(value.start(), outStream);
        instantCoder.encode(value.end(), outStream);
        booleanCoder.encode(value.terminal, outStream);
    }

    @Override public TerminalWindow decode(InputStream inStream) throws CoderException, IOException {
        Instant start = instantCoder.decode(inStream);
        Instant end = instantCoder.decode(inStream);
        Boolean terminal = booleanCoder.decode(inStream);
        return new TerminalWindow(start, end, terminal);
    }

    @Override public List<? extends Coder<?>> getCoderArguments() {
        return Collections.emptyList();
    }

    @Override public void verifyDeterministic() throws NonDeterministicException {}

}
