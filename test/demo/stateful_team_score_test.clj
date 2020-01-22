(ns demo.stateful-team-score-test
  (:require [clojure.test :refer :all]
            [game.stateful-team-score]
            [test-support]
            [thurber :as th])
  (:import (org.joda.time Duration Instant)
           (org.apache.beam.sdk.values TimestampedValue)
           (org.apache.beam.sdk.testing TestStream PAssert TestStream$Builder)
           (org.apache.beam.sdk.transforms.windowing IntervalWindow GlobalWindow Window WindowFn GlobalWindows FixedWindows)))

(def ^:private ^Instant base-time (Instant. 0))

(defn- ^Duration secs [n] (Duration/standardSeconds n))
(defn- ^Duration mins [n] (Duration/standardMinutes n))

(defn- ^TimestampedValue ->event* [event ^Duration offset]
  (let [ts (.plus base-time offset)]
    (TimestampedValue/of (assoc event :timestamp (.getMillis ts)) ts)))

(declare create-scenario*)

(deftest test-score-updates-one-team
  (let [scenario (create-scenario* (GlobalWindows.)
                   (->event* {:user "burgundy" :team "red" :score (int 99)} (secs 10))
                   (->event* {:user "scarlet" :team "red" :score (int 1)} (secs 20))
                   (->event* {:user "scarlet" :team "red" :score (int 0)} (secs 30))
                   (->event* {:user "burgundy" :team "red" :score (int 100)} (secs 40))
                   (->event* {:user "burgundy" :team "red" :score (int 201)} (secs 50)))]
    (-> (PAssert/that scenario)
      (.inWindow GlobalWindow/INSTANCE)
      (.containsInAnyOrder ^Iterable [["red" 100] ["red" 200] ["red" 401]]))
    (test-support/run-test-pipeline! scenario)))

(deftest test-score-updates-per-team
  (let [scenario (create-scenario* (GlobalWindows.)
                   (->event* {:user "burgundy" :team "red" :score (int 50)} (secs 10))
                   (->event* {:user "scarlet" :team "red" :score (int 50)} (secs 20))
                   (->event* {:user "navy" :team "blue" :score (int 70)} (secs 30))
                   (->event* {:user "sky" :team "blue" :score (int 80)} (secs 40))
                   (->event* {:user "sky" :team "blue" :score (int 50)} (secs 50)))]
    (-> (PAssert/that scenario)
      (.inWindow GlobalWindow/INSTANCE)
      (.containsInAnyOrder ^Iterable
        [["red" 100] ["blue" 150] ["blue" 200]]))
    (test-support/run-test-pipeline! scenario)))

(deftest team-score-updates-per-window
  (let [team-window-duration (mins 5)
        window-1 (IntervalWindow. base-time team-window-duration)
        window-2 (IntervalWindow. (.end window-1) team-window-duration)
        scenario (create-scenario* (FixedWindows/of team-window-duration)
                   (->event* {:user "burgundy" :team "red" :score (int 50)} (mins 1))
                   (->event* {:user "scarlet" :team "red" :score (int 50)} (mins 2))
                   (->event* {:user "burgundy" :team "red" :score (int 50)} (mins 3))
                   (->event* {:user "burgundy" :team "red" :score (int 60)} (mins 6))
                   (->event* {:user "scarlet" :team "red" :score (int 60)} (mins 7)))]
    (-> (PAssert/that scenario)
      (.inWindow window-1)
      (.containsInAnyOrder ^Iterable [["red" 100]]))
    (-> (PAssert/that scenario)
      (.inWindow window-2)
      (.containsInAnyOrder ^Iterable [["red" 120]]))
    (test-support/run-test-pipeline! scenario)))

(defn- create-scenario* [^WindowFn window & events]
  (as-> (TestStream/create th/nippy)
    ^TestStream$Builder test-stream
    (.advanceWatermarkTo test-stream base-time)
    (apply test-support/add-elements! test-stream events)
    (.advanceWatermarkToInfinity test-stream)
    (th/apply! (test-support/create-test-pipeline)
      test-stream
      (Window/into window)
      (th/partial #'th/->kv :team)
      (th/partial
        #'game.stateful-team-score/update-team-score 100)
      #'th/kv->clj)))