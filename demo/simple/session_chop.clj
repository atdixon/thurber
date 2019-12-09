(ns simple.session-chop
  (:require [thurber :as th]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.values TupleTag PCollectionTuple KV)
           (thurber.java.exp UnboundedSeqSource TerminalWindowFn)
           (org.joda.time Instant Duration)
           (org.apache.beam.sdk.io Read)
           (org.apache.beam.sdk.transforms.windowing Window TimestampCombiner)
           (org.apache.beam.sdk.transforms Sum GroupByKey)))

(defn- sink* [elem]
  (log/infof "%s @ %s [%s]" elem (.timestamp th/*process-context*) th/*element-window*))

(defn- custom-source []
  (flatten
   [^{:th/event-timestamp (Instant. #inst "2019-01-01T01:00:00")} {:num 1 :user :bob}
    ^{:th/event-timestamp (Instant. #inst "2019-01-01T01:00:30")} {:num 2 :user :bob}
    ^{:th/event-timestamp (Instant. #inst "2019-01-01T01:01:00")} {:num 3 :user :bob}
    ^{:th/event-timestamp (Instant. #inst "2019-01-01T01:04:00")} {:num 4 :user :bob :type :stop}
    ^{:th/event-timestamp (Instant. #inst "2019-01-01T01:05:00")} {:num 5 :user :bob}
    ^{:th/event-delay 5000
      :th/event-timestamp (Instant. #inst "2019-01-01T02:00:00")} {:num 100 :user :bob}]))

(defn- terminal? [elem]
  (= (:type elem) :stop))

(defn- create-pipeline []
  (let [pipeline (th/create-pipeline)
        source (UnboundedSeqSource/create #'custom-source)]
    (th/apply!
     pipeline
     (Read/from source)
     (->
       (Window/into
        (TerminalWindowFn.
         #'terminal?
         (Duration/standardMinutes 5)
         (Duration/standardSeconds 15))))
     (th/partial* #'th/->kv :user)
     (GroupByKey/create)
     #'sink*)
    pipeline))

(defn demo! []
  (-> (create-pipeline)
      (.run)))
