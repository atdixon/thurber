(ns test-support
  (:require [clojure.test :refer :all]
            [thurber :as th])
  (:import (org.apache.beam.sdk.testing TestPipeline TestStream$Builder)
           (org.apache.beam.sdk.values PCollection TimestampedValue)))

(defn create-test-pipeline
  ([] (-> (TestPipeline/create)
        (.enableAbandonedNodeEnforcement true)))
  ([opts] (-> (TestPipeline/fromOptions (th/create-options opts))
            (.enableAbandonedNodeEnforcement true))))

(defn run-test-pipeline! [p]
  (-> ^TestPipeline
    (cond
      (instance? PCollection p) (.getPipeline ^PCollection p)
      (instance? TestPipeline p) p)
    (.run)
    (.waitUntilFinish)))

(defn add-elements! [^TestStream$Builder test-stream & elems]
  (.addElements test-stream
    ^TimestampedValue (first elems)
    ^"[Lorg.apache.beam.sdk.values.TimestampedValue;"
    (into-array TimestampedValue (rest elems))))