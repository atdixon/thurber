(ns simple.session-chop
  (:require [thurber :as th]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.tools.logging :as log])
  (:import (thurber.java.exp UnboundedSeqSource TerminalWindowFn)
           (org.joda.time Instant Duration)
           (org.apache.beam.sdk.io Read)
           (org.apache.beam.sdk.transforms.windowing Window)
           (org.apache.beam.sdk.transforms GroupByKey)
           (org.apache.beam.sdk.values KV)))

(defn- sink* [elem]
  (let [seen-sink (t/now)]
    (log/infof "%s @ (seen = %s) %s [%s]" elem seen-sink (.timestamp th/*process-context*) th/*element-window*)))

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

(defn- mark-now [kw elem]
  (assoc elem kw (t/now)))

(defn- ^{:th/coder th/nippy-kv} mark-now-kv [kw ^KV elem]
  (let [now-inst (t/now)
        k (.getKey elem)
        v (.getValue elem)]
    (KV/of k (assoc v kw now-inst))))

(defn- create-pipeline []
  (let [pipeline (th/create-pipeline)
        source (UnboundedSeqSource/create #'custom-source)]
    (th/apply!
      pipeline
      (Read/from source)
      (th/partial* #'mark-now :pre-window)
      (->
        (Window/into
          (TerminalWindowFn.
            #'terminal?
            (Duration/standardMinutes 5)
            (Duration/millis 1))))
      (th/partial* #'th/->kv :user)
      (th/partial* #'mark-now-kv :pre-group)
      (GroupByKey/create)
      #'sink*)
    pipeline))

(defn demo! []
  (-> (create-pipeline)
    (.run)))
