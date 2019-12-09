package thurber.java.exp;

import clojure.lang.Var;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import thurber.java.Core;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public final class UnboundedSeqSource extends UnboundedSource<Object, UnboundedSeqReader.CustomCheckpointMark> {

    /**
     * The provided <code>seqFn</code> (a {@link Var}) refs a function that produces a {@link clojure.lang.Seqable}.
     * The fn may produce a new seq or memoized seq; the seq may be lazy or not; but the answered seq must be identical
     * each invocation so we can do (index-based) check-pointing properly.
     */
    public static UnboundedSeqSource create(Var seqFn) {
        return new UnboundedSeqSource(seqFn);
    }

    private final Var seqFn;

    private UnboundedSeqSource(Var seqFn) {
        this.seqFn = seqFn;
    }

    @Override
    public List<UnboundedSeqSource> split(int desiredNumSplits, PipelineOptions options) throws Exception {
        return Collections.singletonList(new UnboundedSeqSource(this.seqFn));
    }

    @Override
    public UnboundedReader<Object> createReader(PipelineOptions options, @Nullable UnboundedSeqReader.CustomCheckpointMark checkpoint) throws IOException {
        return new UnboundedSeqReader(this, options, checkpoint, seqFn);
    }

    @Override
    public Coder<Object> getOutputCoder() {
        //noinspection unchecked
        return (Coder<Object>) Core.nippy_.deref();
    }

    @Override
    public Coder<UnboundedSeqReader.CustomCheckpointMark> getCheckpointMarkCoder() {
        return AvroCoder.of(UnboundedSeqReader.CustomCheckpointMark.class);
    }

}
