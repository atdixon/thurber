(ns simple.co-group
  (:require [thurber :as th]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.transforms.join KeyedPCollectionTuple CoGroupByKey CoGbkResult)
           (org.apache.beam.sdk.values PCollection KV)))

;; CoGroupByKey demo

(defn- ^KeyedPCollectionTuple ->keyed-pcoll-tuple [^String name ^PCollection pcoll & nps]
  (reduce (fn [^KeyedPCollectionTuple acc [^String name ^PCollection pcoll]]
            (.and acc name pcoll)) (KeyedPCollectionTuple/of name pcoll) (partition 2 nps)))

(defn- build-pipeline! [pipeline]
  (letfn [(->kv-stream [scope data] (th/apply! scope pipeline (th/create data)
                                      (th/partial "key-by-name" #'th/->kv :name)))]
    (let [email-stream (->kv-stream "email-stream"
                         [{:name :amy :email "amy@thurber"}
                          {:name :carl :email "carl@thurber"}
                          {:name :carl :email "carl@thurber-x"}])
          phone-stream (->kv-stream "phone-stream"
                         [{:name :amy :phone "111-222-3333"}
                          {:name :carl :phone "444-555-6666"}])]
      (th/apply!
        (->keyed-pcoll-tuple "emails" email-stream "phone-numbers" phone-stream)
        (CoGroupByKey/create)
        (th/fn* ^{:th/coder th/nippy} clj-ify* [^KV elem]
          ; note: CoGbkResult Iterable values are lazily provided by Beam!!
          (let [result ^CoGbkResult (.getValue elem)]
            {:name (.getKey elem)
             :emails (into [] (map :email) (.getAll result "emails"))
             :phone-numbers (into [] (map :phone) (.getAll result "phone-numbers"))}))
        #'th/log)
      pipeline)))

(defn demo! []
  (->
    (th/create-pipeline)
    (build-pipeline!)
    (.run)))