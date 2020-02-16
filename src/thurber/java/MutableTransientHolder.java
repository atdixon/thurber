package thurber.java;

import clojure.lang.IEditableCollection;
import clojure.lang.IPersistentCollection;
import clojure.lang.ITransientCollection;

public class TransientHolder {

    ITransientCollection held;

    public TransientHolder(ITransientCollection held) {
        this.held = held;
    }

    public IPersistentCollection asFreezablePersistent() {
        final IPersistentCollection freezable = held.persistent();
        this.held = ((IEditableCollection) freezable).asTransient();
        return freezable;
    }

}
