package thurber.java;

import clojure.lang.IEditableCollection;
import clojure.lang.IPersistentCollection;
import clojure.lang.ITransientCollection;

public class MutableTransientHolder {

    public transient ITransientCollection held;

    public MutableTransientHolder(ITransientCollection held) {
        this.held = held;
    }

    public IPersistentCollection asFinalPersistent() {
        final IPersistentCollection persistent = held.persistent();
        this.held = null;
        return persistent;
    }

    public IPersistentCollection toPersistent() {
        final IPersistentCollection freezable = held.persistent();
        this.held = ((IEditableCollection) freezable).asTransient();
        return freezable;
    }

    @Override public int hashCode() {
        return held.hashCode();
    }

    @Override public boolean equals(Object obj) {
        if (obj instanceof MutableTransientHolder) {
            // todo - consider mutations going thru this holder to properly invalidate and do equals etc
            MutableTransientHolder other = (MutableTransientHolder) obj;
            return toPersistent().equals(other.toPersistent());
        }
        return false;
    }
}
