(ns simple.sum-and-count
  (:require [thurber :as th]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.transforms Combine)))

;; Simple demonstration of combine.

(defn- sink* [elem]
  (log/info elem))

(defn- reducef
  ([] {:sum 0 :count 0})
  ([acc inp] (-> acc
               (update :sum + inp)
               (update :count inc))))

(defn- combinef [& accs]
  (apply merge-with + accs))

(defn- build-pipeline! [pipeline]
  (let [data (th/apply! pipeline (th/create [1 2 3 4 5]))]
    (th/apply!
      data
      (Combine/globally
        (th/combiner #'combinef #'reducef))
      #'sink*)
    (th/apply!
      data
      (Combine/globally
        (th/combiner #'+))
      {:th/name "sink-2"
       :th/xform #'sink*})
    pipeline))

(defn demo! []
  (-> (th/create-pipeline) build-pipeline! .run))

