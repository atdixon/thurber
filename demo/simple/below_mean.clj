(ns simple.below-mean
  (:require [thurber :as th]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.transforms Mean View)))

;; Simple demonstration of using side inputs.

(defn- sink* [elem]
  (log/info elem))

(defn- below-mean? [mean-view elem]
  (let [mean (.sideInput (th/*process-context) mean-view)]
    (< elem mean)))

(defn- build-pipeline! [pipeline]
  (let [data (th/apply! pipeline (th/create [1 2 3 4 5 6 7 8]))
        mean-view (th/apply!
                    data
                    (Mean/globally)
                    (View/asSingleton))]
    (th/apply!
      data
      (th/filter* #'below-mean? mean-view)
      #'sink*)
    pipeline))

(defn demo! []
  (->
    (th/create-pipeline)
    (build-pipeline!)
    (.run)))

