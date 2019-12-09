package thurber.java.exp;

import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.joda.time.Instant;

import java.util.Objects;

public class TerminalWindow extends IntervalWindow {

    final Boolean terminal;

    public TerminalWindow(Instant start, Instant end, Boolean terminal) {
        super(start, end);
        this.terminal = terminal;
    }

    public TerminalWindow span(TerminalWindow other) {
        return new TerminalWindow(
            new Instant(Math.min(start().getMillis(), other.start().getMillis())),
            new Instant(
                this.terminal ? end().getMillis() :
                    other.terminal ? other.end().getMillis()
                        : Math.max(end().getMillis(), other.end().getMillis())),
            this.terminal || other.terminal);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), terminal);
    }

    @Override public boolean equals(Object obj) {
        if (!(obj instanceof TerminalWindow))
            return false;
        TerminalWindow other = (TerminalWindow) obj;
        return super.equals(obj)
            && Objects.equals(this.terminal, other.terminal);
    }

}
