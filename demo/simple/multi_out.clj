(ns simple.multi-out
  (:require [thurber :as th]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.values TupleTag PCollectionTuple)))

;; Simple demonstration of using multi-output/tags.

(defn- sink* [pfx elem]
  (log/infof "[%s] %s" pfx elem))

(defn- output* [^TupleTag even-tag ^TupleTag odd-tag elem]
  (if (even? elem)
    (.output (th/*process-context) even-tag {:value elem})
    (.output (th/*process-context) odd-tag {:value elem})))

(defn- build-pipeline! [pipeline]
  (let [data (th/apply! pipeline (th/create [1 2 3 4 5 6 7 8]))
        even-tag (TupleTag. "even")
        odd-tag (TupleTag. "odd")
        pcolls ^PCollectionTuple
               (th/apply! data (th/partial #'output* even-tag odd-tag))]
    (th/apply!
      (.get pcolls "even")
      (th/partial #'sink* "even"))
    (th/apply!
      (.get pcolls "odd")
      (th/partial #'sink* "odd"))
    pipeline))

(defn demo! []
  (-> (th/create-pipeline) build-pipeline! .run))

