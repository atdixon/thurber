package thurber.java.exp;

import clojure.lang.*;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;
import thurber.java.Core;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.Serializable;
import java.util.NoSuchElementException;

/** {@link org.apache.beam.sdk.io.UnboundedSource.UnboundedReader} need not be thread-safe; they are always accessed
 * by a single thread. */
@NotThreadSafe
public final class UnboundedSeqReader extends UnboundedSource.UnboundedReader<Object> {

    /** CheckpointMark. */
    static final class CustomCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {
        private final int numRead;

        /** For deserializer. */
        CustomCheckpointMark() {
            this.numRead = -1;
        }

        CustomCheckpointMark(int numRead) {
            this.numRead = numRead;
        }

        @Override
        public void finalizeCheckpoint() throws IOException {}

    }

    private final UnboundedSource<Object, ?> source;
    private final PipelineOptions options;
    private final CustomCheckpointMark checkpoint;
    private final Var seqFn;

    private boolean started, paused;
    private ISeq seq;
    private int numRead;
    private Object current; // most recently exposed/released/getCurrent value
    private long resumes = System.currentTimeMillis();

    UnboundedSeqReader(UnboundedSource<Object, ?> source, PipelineOptions options, CustomCheckpointMark checkpoint, Var seqFn) {
        this.source = source;
        this.options = options;
        this.checkpoint = checkpoint;
        this.seqFn = seqFn;
    }

    @Override
    public boolean start() throws IOException {
        Core.require_(seqFn);
        this.started = true;
        this.seq = RT.seq(seqFn.invoke());
        if (this.seq != null && checkpoint != null)
            for (int i = 0; i < checkpoint.numRead && advance(); ++i) /*no-op*/ ;
        if (this.seq == null) {
            this.paused = false;
            return false; // seq is DEAD; getWatermark will say MAX, ending everything.
        }
        ++numRead;
        Object curr = this.seq.first();
        @Nullable Duration delay = toEventDelay(curr);
        if (delay != null) {
            this.resumes = System.currentTimeMillis() + delay.getMillis();
            this.paused = true;
        }
        return !this.paused;
    }

    @Override
    public boolean advance() throws IOException {
        // FOR NOW: if we return false we are at end of sequence, so we HAPPEN to know
        //    that no more items are coming ...
        if (this.seq == null)
            // seq ended; we know we'll never see more
            return false;
        if (this.paused && this.resumes > System.currentTimeMillis())
            // we see nothing
            return false;
        if (this.paused) {
            this.paused = false; // unpause
            // we were paused seq is already "on" paused elem
            return true;
        }
        // we were not paused; we need to advance for real.
        this.seq = this.seq.next();
        if (null == this.seq) {
            this.paused = false;
            return false; // seq is DEAD; ...
        }
        ++numRead;
        Object curr = this.seq.first();
        @Nullable Duration delay = toEventDelay(curr);
        if (delay != null) {
            this.resumes = System.currentTimeMillis() + delay.getMillis();
            this.paused = true;
        }
        return !this.paused;
    }

    @Override
    public Object getCurrent() throws NoSuchElementException {
        if (this.seq == null)
            throw new NoSuchElementException();
        if (this.paused)
            throw new IllegalStateException("unexpected");
        return this.current = this.seq.first();
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (this.seq == null)
            throw new NoSuchElementException();
        if (this.paused)
            throw new IllegalStateException("unexpected?");
        // NOTE: VERY CAREFUL TO IMPLEMENT THIS PER SEMANTICS OF getCurrentTimestamp + the fact we're Unbounded
        Object useObj = getCurrent();
        if (useObj == null)
            throw new NoSuchElementException();
        @Nullable Instant ts = toEventTimestamp(useObj);
        if (ts != null)
            return ts;
        throw new RuntimeException(":th/event-timestamp always req'd");
    }

    @Override
    public void close() throws IOException {}

    @Override
    public Instant getWatermark() {
        // be verrry careful to get this right -- HINT: it is not the same as getCurrentTimestamp
        if (!started)
            return BoundedWindow.TIMESTAMP_MIN_VALUE;
        if (this.seq == null) //NOTE: we are unbounded but doing this here lets runner end the pipeline:
            return BoundedWindow.TIMESTAMP_MAX_VALUE; // end of seq
        if (this.current != null) {
            Instant ts = toEventTimestamp(this.current);
            if (ts == null)
                throw new RuntimeException(":th/event-timestamp always req'd");
            return ts;
        }
        if (this.seq.first() != null) {
            // we've started or advanced but Beam did not call getCurrent yet? FINE.
            Instant ts = toEventTimestamp(this.seq.first());
            if (ts == null)
                throw new RuntimeException(":th/event-timestamp always req'd");
            return ts;
        }
        throw new IllegalStateException("unexpected?");
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
        return new CustomCheckpointMark(numRead);
    }

    @Override
    public UnboundedSource<Object, ?> getCurrentSource() {
        return this.source;
    }

    @Nullable
    private static Duration toEventDelay(Object obj) {
        if (obj instanceof IMeta) {
            final Object et =
                ((IMeta) obj).meta().valAt(Keyword.intern("th", "event-delay"));
            if (et != null)
                return Duration.millis((Long) et);
        }
        return null;
    }

    @Nullable
    private static Instant toEventTimestamp(Object obj) {
        if (obj instanceof IMeta) {
            final Object et =
                ((IMeta) obj).meta().valAt(Keyword.intern("th", "event-timestamp"));
            if (et != null)
                return (Instant) et;
        }
        return null;
    }

}
