(ns simple.proxy-foo
  (:require [thurber :as th]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.transforms Combine Combine$CombineFn)))

;;
;; `th/proxy*` can be leveraged to cover scenarios not yet covered by
;;   thurber's primary api.
;;
;; Here, we need to provide serializable args to a CombineFn much
;;   like `th/partial*` supports providing args to ParDo Clojure functions.
;;
;; @see https://github.com/atdixon/thurber/issues/3
;;
;; Basically `th/proxy*` wraps a Clojure proxy such that it can be serialized
;;   as is required by certain interfaces in Beam (like CombineFn).
;;

(def combine-with-config-args
  (proxy [Combine$CombineFn] []
    (createAccumulator [] {})
    (addInput [acc inp]
      ;; Here is where we use the custom args provided to our proxy:
      (let [{:keys [custom-unit-val custom-aggregator] :as args-conf_} (-> (th/*proxy-args) first)]
        (-> acc
          (update :sum (fnil + 0) inp)
          (update :count (fnil inc 0))
          (update :custom-agg
            (fnil custom-aggregator custom-unit-val) inp))))
    (mergeAccumulators [^Iterable accs]
      (let [{:keys [custom-unit-val custom-aggregator] :as args-conf_} (-> (th/*proxy-args) first)]
        (reduce (fn [memo acc]
                  (assoc
                    (merge-with + memo (select-keys acc [:sum :count]))
                    :custom-agg (custom-aggregator
                                  (or (:custom-agg memo) custom-unit-val)
                                  (or (:custom-agg acc) custom-unit-val)))) {} accs)))
    (extractOutput [acc] acc)
    (getAccumulatorCoder [reg_ inp-coder_] th/nippy)
    (getDefaultOutputCoder [reg_ inp-coder_] th/nippy)))

(defn- build-pipeline! [pipeline]
  (let [data (th/apply! pipeline (th/create [1 2 3 4 5]))]
    (th/apply!
      data
      (Combine/globally
        (th/proxy* #'combine-with-config-args
          ;; The args we pass to our proxy must be serializable.
          ;; Here we pass in a clojure.core/* Var to be our custom aggregator.
          {:custom-unit-val 1 :custom-aggregator #'*}))
      #'th/log-verbose)
    pipeline))

(defn demo! []
  (->
    (th/create-pipeline)
    (build-pipeline!)
    (.run)))

