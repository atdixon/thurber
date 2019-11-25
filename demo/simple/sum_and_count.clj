(ns simple.sum-and-count
  (:require [thurber :as th]
            [clojure.tools.logging :as log])
  (:import (thurber CombineFn)
           (thurber.java TCombine)
           (org.apache.beam.sdk.transforms Combine)))

(defn- sink* [elem]
  (log/info elem))

(def ^:private sum-and-count-xf
  (th/def-combine
    (create-accumulator [_] {:sum 0 :count 0})
    (add-input [_ acc input]
      (-> acc
        (update :sum + input)
        (update :count inc)))
    (merge-accumulators [_ coll] (apply merge-with + coll))
    (extract-output [_ acc] acc)))

(defn- create-pipeline [opts]
  (let [pipeline (th/create-pipeline opts)
        conf (th/get-custom-config pipeline)]
    (doto pipeline
      (th/apply!
        (th/create* [1 2 3 4 5])
        (th/combine-globally #'sum-and-count-xf)
        #'sink*))))

(defn demo! []
  (-> (create-pipeline
        ;; Thurber fully supports Beam's PipelineOptions and static Java interfaces.
        ;;
        ;; Thurber also supports Clojure/EDN maps for providing options; core Beam
        ;; options are provided by their standard names (as skeleton case); config
        ;; unique to your pipeline can be specified under :custom-config.
        ;;
        ;; Config provided this way must be serializable to JSON (per Beam).
        {:target-parallelism 25
         :custom-config {:input-file "lorem.txt"}})
    (.run)))

