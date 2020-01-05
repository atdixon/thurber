package thurber.java.exp;

import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.joda.time.Instant;

import java.util.Objects;

/**
 * An {@link IntervalWindow} with an extra bit to indicate whether it is
 * <code>terminal</code> or not.
 * <p>
 * This class supports window merging via its {@link #span(TerminalWindow)}} method;
 * terminal windows refuse to merge/span beyond their {@link IntervalWindow#end()}.
 * In this sense <code>terminal</code> windows dictate when the final merged window
 * will close.
 *
 * @see TerminalWindowMerge
 */
public final class TerminalWindow extends IntervalWindow {

    final Boolean terminal;

    public TerminalWindow(Instant start, Instant end, Boolean terminal) {
        super(start, end);
        this.terminal = terminal;
    }

    public TerminalWindow span(TerminalWindow other) {
        return new TerminalWindow(
            new Instant(Math.min(start().getMillis(), other.start().getMillis())),
            new Instant(
                this.terminal && other.terminal
                    ? Math.min(end().getMillis(), other.end().getMillis())
                    : this.terminal ? end().getMillis()
                    : other.terminal ? other.end().getMillis()
                    : Math.max(end().getMillis(), other.end().getMillis())),
            this.terminal || other.terminal);
    }

    @Override
    public String toString() {
        return "{" + super.toString() + (terminal ? ",terminal" : "") + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), terminal);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TerminalWindow))
            return false;
        TerminalWindow other = (TerminalWindow) obj;
        return super.equals(obj)
            && Objects.equals(this.terminal, other.terminal);
    }

}
