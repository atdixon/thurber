package thurber.java.exp;

import clojure.java.api.Clojure;
import clojure.lang.PersistentArrayMap;
import clojure.lang.Var;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import thurber.java.Core;

import java.io.IOException;
import java.util.Collections;

public class TFilenamePolicy extends FileBasedSink.FilenamePolicy {

    private final Var delegate;
    private final Object config;

    public TFilenamePolicy(Object config, Var delegate) {
        this.config = config;
        this.delegate = delegate;
    }

    @Override
    public ResourceId windowedFilename(int shardNumber, int numShards, BoundedWindow window, PaneInfo paneInfo, FileBasedSink.OutputFileHints outputFileHints) {
        Var.pushThreadBindings(PersistentArrayMap.create(
            Collections.singletonMap((Var) Clojure.var("thurber", "*proxy-config*"), config)));
        try {
            return ((FileBasedSink.FilenamePolicy) delegate.deref())
                .windowedFilename(shardNumber, numShards, window, paneInfo, outputFileHints);
        } finally {
            Var.popThreadBindings();
        }
    }

    @Override
    public ResourceId unwindowedFilename(int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
        Var.pushThreadBindings(PersistentArrayMap.create(
            Collections.singletonMap((Var) Clojure.var("thurber", "*proxy-config*"), config)));
        try {
            return ((FileBasedSink.FilenamePolicy) delegate.deref())
                .unwindowedFilename(shardNumber, numShards, outputFileHints);
        } finally {
            Var.popThreadBindings();
        }
    }

    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        Core.require_(this.delegate);
    }

}
