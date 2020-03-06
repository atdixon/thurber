(ns simple.combine-with-partial
  (:require [thurber :as th])
  (:import (org.apache.beam.sdk.transforms Combine)))

(defn- build-pipeline! [pipeline]
  (let [data (th/apply! pipeline (th/create [1 2 3 4 5]))
        custom-aggregator #'*
        custom-unit-val 1]
    (th/apply!
      data
      (Combine/globally
        (th/combiner
          (th/fn* agg-combinef
            [& accs]
            (reduce
              (fn [memo acc]
                (assoc
                  (merge-with + memo (select-keys acc [:sum :count]))
                  :custom-agg (custom-aggregator
                                (or (:custom-agg memo) custom-unit-val)
                                (or (:custom-agg acc) custom-unit-val)))) {} accs))
          (th/fn* agg-reducef
            ([] {})
            ([acc inp]
             (-> acc
               (update :sum (fnil + 0) inp)
               (update :count (fnil inc 0))
               (update :custom-agg
                 (fnil custom-aggregator custom-unit-val) inp))))))
      #'th/log-verbose)
    pipeline))

(defn demo! []
  (-> (th/create-pipeline) build-pipeline! .run))

