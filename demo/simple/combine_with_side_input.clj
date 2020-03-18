(ns simple.combine-with-side-input
  (:require [thurber :as th])
  (:import (org.apache.beam.sdk.transforms Combine View Min)))

;; Counts the number of occurrences of the overall minimum value in a data stream.

(defn- build-pipeline! [pipeline]
  (let [data (th/apply! pipeline (th/create [5 8 5 5 6 88 100 5 23 5 49 5]))
        min-view (th/apply!
                   data
                   (Min/globally)
                   (View/asSingleton))]
    (th/apply!
      data
      (->
        (Combine/globally
          (th/combiner
            (th/fn* agg-extractf
              ;; Note: .withoutDefaults must be used in any global Combine that uses
              ;;   us or else we may see our *process-context/*custom-config empty/nil
              ;;   here when the global Combine attempts to produce a defaultValue.
              ;; With .withoutDefaults we don't have to check for nil *custom-config here:
              [x] (* x (:result-multiplier (th/*custom-config))))
            (th/fn* agg-combinef
              [& accs]
              (reduce + 0 accs))
            (th/fn* agg-reducef
              ([] 0)
              ([acc inp]
               (let [min-val (th/*side-input min-view)]
                 (cond-> acc (= inp min-val) inc))))))
        (.withSideInputs [min-view])
        (.withoutDefaults))
      #'th/log-verbose)
    pipeline))

(defn demo! []
  (-> (th/create-pipeline {:custom-config {:result-multiplier 10}}) build-pipeline! .run))
