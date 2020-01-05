(ns simple.looping-timer
  (:require [thurber :as th]
            [clojure.tools.logging :as log])
  (:import (org.joda.time Instant Duration)
           (org.apache.beam.sdk.transforms.windowing Window FixedWindows GlobalWindows)
           (org.apache.beam.sdk.values KV TimestampedValue)
           (org.apache.beam.sdk.transforms Create Sum)))

; https://beam.apache.org/blog/2019/06/11/looping-timers.html

(def now (Instant/parse "2000-01-01T00:00:59Z"))

(def time-1 (TimestampedValue/of (KV/of "Key_A" 1) now))
(def time-2 (TimestampedValue/of (KV/of "Key_A" 2)
              (.plus now (Duration/standardMinutes 1))))
(def time-3 (TimestampedValue/of (KV/of "Key_A" 3)
              (.plus now (Duration/standardMinutes 3))))

(defn- sink* [elem]
  (log/infof "Value is %s timestamp is %s" elem (.timestamp (th/*process-context))))

(defn- looping-stateful-fn [^KV kv-elem]
  (let [{:keys [elem-key current-timer-val :as curr-state]} (.read (th/*value-state))
        next-timer-val (-> (.timestamp (th/*process-context))
                         (.plus (Duration/standardMinutes 1)))
        next-state (cond-> curr-state
                     (not elem-key) (assoc :elem-key (.getKey kv-elem))
                     (or (nil? current-timer-val)
                       (> current-timer-val (.getMillis next-timer-val)))
                     (assoc :current-timer-val (.getMillis next-timer-val)))]
    (when (not= current-timer-val next-timer-val)
      (.set (th/*event-timer) next-timer-val))
    (when (not= curr-state next-state)
      (.write (th/*value-state) next-state))
    kv-elem))

(defn- looping-stateful-on-timer-fn [^Instant stop-timer-time]
  (let [ts (.timestamp (th/*timer-context))
        elem-key (:elem-key (.read (th/*value-state)))
        next-timer-time (-> (.timestamp (th/*timer-context))
                          (.plus (Duration/standardMinutes 1)))]
    (log/infof "Timer @ %s fired" ts)
    (when (.isBefore next-timer-time stop-timer-time)
      (.set (th/*event-timer) next-timer-time))
    (KV/of elem-key 0)))

(defn- create-pipeline []
  (doto (th/create-pipeline)
    (th/apply!
      {:th/xform (Create/timestamped [time-1 time-2 time-3])
       :th/coder th/nippy-kv}
      (Window/into
        (FixedWindows/of (Duration/standardMinutes 1)))
      (Sum/longsPerKey)
      (Window/into (GlobalWindows.))
      {:th/xform #'looping-stateful-fn
       :th/timer-fn #'looping-stateful-on-timer-fn
       :th/timer-params [(Instant/parse "2000-01-01T00:04:00Z")]
       :th/coder th/nippy-kv}
      (Window/into
        (FixedWindows/of (Duration/standardMinutes 1)))
      (Sum/longsPerKey)
      #'sink*)))

(defn demo! []
  (-> (create-pipeline)
    (.run)))
