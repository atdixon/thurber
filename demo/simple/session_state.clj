(ns simple.session-state
  (:require [thurber :as th]
            [clojure.tools.logging :as log])
  (:import (thurber.java.exp UnboundedSeqSource)
           (org.joda.time Instant Duration)
           (org.apache.beam.sdk.io Read)
           (org.apache.beam.sdk.transforms.windowing Window GlobalWindows)
           (org.apache.beam.sdk.values KV)))

(defn- sink* [elem]
  (log/infof "%s @ %s [%s]" elem (.timestamp (th/*process-context)) (th/*element-window)))

(defn- custom-source []
  (flatten
    [^{:th/event-timestamp (Instant. #inst "2019-01-01T01:00:00")} {:num 1 :user :bob}
     ^{:th/event-timestamp (Instant. #inst "2019-01-01T01:00:00")} {:num 10 :user :cat}
     ^{:th/event-timestamp (Instant. #inst "2019-01-01T01:00:15")} {:num 20 :user :cat}
     ^{:th/event-timestamp (Instant. #inst "2019-01-01T01:00:30")} {:num 2 :user :bob}
     ^{:th/event-timestamp (Instant. #inst "2019-01-01T01:01:00")} {:num 3 :user :bob}
     ^{:th/event-timestamp (Instant. #inst "2019-01-01T01:04:00")} {:num 4 :user :bob :type :stop}
     ^{:th/event-delay 1000
       :th/event-timestamp (Instant. #inst "2019-01-01T01:04:05")} {:num 5 :user :bob}
     ^{:th/event-timestamp (Instant. #inst "2019-01-01T01:05:00")} {:num 30 :user :cat :type :stop}
     ^{:th/event-timestamp (Instant. #inst "2019-01-01T01:06:02")} {:num 6 :user :bob}
     ^{:th/event-delay 5000
       :th/event-timestamp (Instant. #inst "2019-01-01T01:06:02")} {:num 7 :user :bob}
     ^{:th/event-timestamp (Instant. #inst "2019-01-01T01:06:03")} {:num 40 :user :cat}
     ^{:th/event-delay 5000
       :th/event-timestamp (Instant. #inst "2019-01-01T02:00:00")} {:num 100 :user :bob}]))

(def gap-minutes 30)
(def grace-minutes 1)

(defn- special* [^KV kv]
  (let [elem (.getValue kv)
        state (or (.read (th/*value-state)) {:buffer []
                                            :stop-ts nil})
        state' (-> state
                 (update :buffer conj elem)
                 (update :stop-ts #(or %
                                     (when (= (:type elem) :stop)
                                       (.timestamp (th/*process-context))))))]
    ;; Note: to fully support grace and gap we would have to split our buffer and keep track of
    ;;       our own windows and on timer then flush and clear those that are before the timer
    ;;       timestamp (ie watermark).
    (.write (th/*value-state) state')
    (if-let [stop-ts ^Instant (:stop-ts state')]
      (.set (th/*event-timer)
        (-> stop-ts
          (.plus (Duration/standardMinutes grace-minutes))))
      (.set (th/*event-timer)
        (-> (.timestamp (th/*process-context))
          (.plus (Duration/standardMinutes gap-minutes)))))))

(defn- special-timer* []
  (when-let [state (.read (th/*value-state))]
    (.clear (th/*value-state))
    (:buffer state)))

(defn- build-pipeline! [pipeline]
  (let [source (UnboundedSeqSource/create #'custom-source)]
    (th/apply!
      pipeline
      (Read/from source)
      (->
        (Window/into
          (GlobalWindows.)))
      (th/partial* #'th/->kv :user)
      {:th/xform #'special*
       :th/timer-fn #'special-timer*
       :th/coder th/nippy}
      #'sink*)
    pipeline))

(defn demo! []
  (->
    (th/create-pipeline)
    (build-pipeline!)
    (.run)))
