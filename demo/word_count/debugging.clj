(ns word-count.debugging
  (:require [thurber :as th]
            [word-count.basic]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.io TextIO)
           (org.apache.beam.sdk.testing PAssert)
           (org.apache.beam.sdk.metrics Metrics Counter)
           (org.apache.beam.sdk.values KV)))

;; While Apache Beam has less restrictions on metrics naming, Google StackDriver will not
;; capture/show metrics that don't follow naming rules:
;; https://cloud.google.com/monitoring/api/v3/metrics-details#label_names
(def ^:private ^Counter matched-words (Metrics/counter "word_count.debugging" "matched_words"))
(def ^:private ^Counter unmatched-words (Metrics/counter "word_count.debugging" "unmatched_words"))

(defn- build-pipeline! [pipeline]
  (let [conf (th/get-custom-config pipeline)]
    (->
      (th/apply! pipeline
        (-> (TextIO/read)
          (.from ^String (:input-file conf)))
        word-count.basic/count-words-xf
        (let [pattern (re-pattern (:filter-pattern conf))]
          (th/fn* filter-per-pattern [^KV elem]
            (let [[key- val-] (th/kv->clj* elem)]
              (if (re-matches pattern key-)
                (do
                  (.inc matched-words)
                  (log/debugf "Matched: %s" key-)
                  elem)
                (do
                  (.inc unmatched-words)
                  (log/tracef "Did not match: %s" key-))))))
        #'th/kv->clj)
      (as-> result
        (-> (PAssert/that result)
          (.containsInAnyOrder [["pain" 5] ["pleasure" 7]]))))
    pipeline))

(defn demo! []
  (->
    (th/create-pipeline
      {:custom-config {:input-file "demo/word_count/lorem.txt"
                       :filter-pattern "pain|pleasure"}})
    build-pipeline! .run))
