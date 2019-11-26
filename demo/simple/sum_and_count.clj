(ns simple.sum-and-count
  (:require [thurber :as th]
            [clojure.tools.logging :as log]))

(defn- sink* [elem]
  (log/info elem))

(def ^:private sum-and-count-xf
  (th/def-combiner
    (create-accumulator [_] {:sum 0 :count 0})
    (add-input [_ acc input]
      (-> acc
        (update :sum + input)
        (update :count inc)))
    (merge-accumulators [_ coll] (apply merge-with + coll))
    (extract-output [_ acc] acc)))

(defn- create-pipeline []
  (let [pipeline (th/create-pipeline)
        data (th/apply! pipeline (th/create* [1 2 3 4 5]))]
    (th/apply!
      data
      (th/combine-globally #'sum-and-count-xf)
      #'sink*)
    (th/apply!
      data
      (th/combine-globally #'+)
      #'sink*)
    pipeline))

(defn demo! []
  (-> (create-pipeline)
    (.run)))

