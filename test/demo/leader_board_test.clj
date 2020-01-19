(ns demo.leader-board-test
  (:require [clojure.test :refer :all]
            [thurber :as th]
            [game.leader-board]
            [test-support])
  (:import (org.apache.beam.sdk.testing TestStream TestStream$Builder PAssert)
           (org.joda.time Instant Duration)
           (org.apache.beam.sdk.values TimestampedValue)
           (org.apache.beam.sdk.transforms.windowing IntervalWindow)))

(def ^:private ^Instant base-time (Instant. 0))

(def ^:private ^Duration allowed-lateness-minutes 60)
(def ^:private ^Duration team-window-duration-minutes 20)

(defn- ^Duration secs [n] (Duration/standardSeconds n))
(defn- ^Duration mins [n] (Duration/standardMinutes n))
(defn- ^Duration hours [n] (Duration/standardHours n))

(defn- ^TimestampedValue ->event* [event ^Duration offset]
  (let [ts (.plus base-time offset)]
    (TimestampedValue/of (assoc event :timestamp (.getMillis ts)) ts)))

(deftest test-team-scores-on-time
  (let [pipeline (test-support/create-test-pipeline)
        event-stream (-> (TestStream/create th/nippy)
                       (.advanceWatermarkTo base-time)
                       (test-support/add-elements!
                         (->event* {:user "navy" :team "blue" :score (int 3)} (secs 3))
                         (->event* {:user "navy" :team "blue" :score (int 2)} (mins 1))
                         (->event* {:user "burgundy" :team "red" :score (int 3)} (secs 22))
                         (->event* {:user "sky" :team "blue" :score (int 5)} (mins 3)))
                       (.advanceWatermarkTo (.plus base-time (mins 3)))
                       (test-support/add-elements!
                         (->event* {:user "scarlet" :team "red" :score (int 1)} (mins 4))
                         (->event* {:user "navy" :team "blue" :score (int 2)} (secs 270)))
                       (.advanceWatermarkToInfinity))
        team-scores (th/apply! pipeline
                      event-stream
                      (#'game.leader-board/->calculate-team-scores-xf
                        {:team-window-duration team-window-duration-minutes
                         :allowed-lateness allowed-lateness-minutes}))]
    (-> (PAssert/that team-scores)
      (.inOnTimePane (IntervalWindow. base-time (Duration/standardMinutes
                                                  team-window-duration-minutes)))
      (.containsInAnyOrder ^Iterable [["blue" (int 12)] ["red" (int 4)]]))
    (test-support/run-test-pipeline! pipeline)))


(deftest test-team-scores-speculative
  (let [pipeline (test-support/create-test-pipeline)
        event-stream (-> (TestStream/create th/nippy)
                       (.advanceWatermarkTo base-time)
                       (test-support/add-elements!
                         (->event* {:user "navy" :team "blue" :score (int 3)} (secs 3))
                         (->event* {:user "navy" :team "blue" :score (int 2)} (mins 1)))
                       (.advanceProcessingTime (mins 10))
                       (test-support/add-elements!
                         (->event* {:user "burgundy" :team "red" :score (int 5)} (mins 3)))
                       (.advanceProcessingTime (mins 12))
                       (test-support/add-elements!
                         (->event* {:user "sky" :team "blue" :score (int 3)} (secs 22)))
                       (.advanceProcessingTime (mins 10))
                       (test-support/add-elements!
                         (->event* {:user "scarlet" :team "red" :score (int 4)} (mins 4))
                         (->event* {:user "sky" :team "blue" :score (int 2)} (mins 2)))
                       (.advanceWatermarkToInfinity))
        team-scores (th/apply! pipeline
                      event-stream
                      (#'game.leader-board/->calculate-team-scores-xf
                        {:team-window-duration team-window-duration-minutes
                         :allowed-lateness allowed-lateness-minutes}))]
    (-> (PAssert/that team-scores)
      (.inWindow (IntervalWindow. base-time (Duration/standardMinutes
                                              team-window-duration-minutes)))
      (.containsInAnyOrder ^Iterable [["blue" (int 10)]
                                      ["red" (int 9)]
                                      ["blue" (int 5)]
                                      ["blue" (int 8)]
                                      ["red" (int 5)]]))
    (-> (PAssert/that team-scores)
      (.inOnTimePane (IntervalWindow. base-time (Duration/standardMinutes
                                                  team-window-duration-minutes)))
      (.containsInAnyOrder ^Iterable [["blue" (int 10)] ["red" (int 9)]]))
    (test-support/run-test-pipeline! pipeline)))

(deftest test-team-scores-unobservably-late
  (let [pipeline (test-support/create-test-pipeline)
        event-stream (-> (TestStream/create th/nippy)
                       (.advanceWatermarkTo base-time)
                       (test-support/add-elements!
                         (->event* {:user "navy" :team "blue" :score (int 3)} (secs 3))
                         (->event* {:user "sky" :team "blue" :score (int 5)} (mins 8))
                         (->event* {:user "scarlet" :team "red" :score (int 4)} (mins 2))
                         (->event* {:user "navy" :team "blue" :score (int 3)} (mins 5)))
                       (.advanceWatermarkTo (-> base-time
                                              (.plus (mins team-window-duration-minutes))
                                              (.minus (mins 1))))
                       (test-support/add-elements!
                         (->event* {:user "burgundy" :team "red" :score (int 2)} Duration/ZERO)
                         (->event* {:user "burgundy" :team "red" :score (int 5)} (mins 1))
                         (->event* {:user "sky" :team "blue" :score (int 2)} (secs 90))
                         (->event* {:user "burgundy" :team "red" :score (int 3)} (mins 3)))
                       (.advanceWatermarkTo (-> base-time
                                              (.plus (mins team-window-duration-minutes))
                                              (.plus (mins 1))))
                       (.advanceWatermarkToInfinity))
        team-scores (th/apply! pipeline
                      event-stream
                      (#'game.leader-board/->calculate-team-scores-xf
                        {:team-window-duration team-window-duration-minutes
                         :allowed-lateness allowed-lateness-minutes}))]
    (-> (PAssert/that team-scores)
      (.inOnTimePane (IntervalWindow. base-time (Duration/standardMinutes
                                                  team-window-duration-minutes)))
      (.containsInAnyOrder ^Iterable [["red" (int 14)] ["blue" (int 13)]]))
    (test-support/run-test-pipeline! pipeline)))

(deftest test-team-scores-observably-late
  (let [pipeline (test-support/create-test-pipeline)
        first-window-closes (-> base-time
                              (.plus (mins allowed-lateness-minutes))
                              (.plus (mins team-window-duration-minutes)))
        event-stream (-> (TestStream/create th/nippy)
                       (.advanceWatermarkTo base-time)
                       (test-support/add-elements!
                         (->event* {:user "navy" :team "blue" :score (int 3)} (secs 3))
                         (->event* {:user "sky" :team "blue" :score (int 5)} (mins 8)))
                       (.advanceProcessingTime (mins 10))
                       (.advanceWatermarkTo (-> base-time
                                              (.plus (mins 3))))
                       (test-support/add-elements!
                         (->event* {:user "scarlet" :team "red" :score (int 3)} (mins 1))
                         (->event* {:user "scarlet" :team "red" :score (int 4)} (mins 2))
                         (->event* {:user "navy" :team "blue" :score (int 3)} (mins 5)))
                       (.advanceWatermarkTo (-> first-window-closes
                                              (.minus (mins 1))))
                       (test-support/add-elements!
                         (->event* {:user "burgundy" :team "red" :score (int 2)} Duration/ZERO)
                         (->event* {:user "burgundy" :team "red" :score (int 5)} (mins 1))
                         (->event* {:user "burgundy" :team "red" :score (int 3)} (mins 3)))
                       (.advanceProcessingTime (mins 12))
                       (test-support/add-elements!
                         (->event* {:user "burgundy" :team "red" :score (int 9)} (mins 1))
                         (->event* {:user "burgundy" :team "red" :score (int 1)} (mins 3)))
                       (.advanceWatermarkToInfinity))
        team-scores (th/apply! pipeline
                      event-stream
                      (#'game.leader-board/->calculate-team-scores-xf
                        {:allowed-lateness allowed-lateness-minutes
                         :team-window-duration team-window-duration-minutes}))
        window (IntervalWindow. base-time (mins team-window-duration-minutes))]
    (-> (PAssert/that team-scores)
      (.inWindow window)
      (.satisfies (th/simple*
                    (th/inline
                      (fn test-team-scores-observably-late-assert [input]
                        (is (some #{["blue" (int 11)]} input))
                        (is (some #{["red" (int 27)]} input)))))))
    (-> (PAssert/thatMap
          (th/apply! team-scores #'th/clj->kv))
      (.inOnTimePane window)
      (.isEqualTo {"red" (int 7) "blue" (int 11)}))
    (-> (PAssert/that team-scores)
      (.inFinalPane window)
      (.containsInAnyOrder [["red" (int 27)]]))
    (test-support/run-test-pipeline! pipeline)))

(deftest test-team-scores-droppably-late
  (let [pipeline (test-support/create-test-pipeline)
        window (IntervalWindow. base-time (mins team-window-duration-minutes))
        event-stream (-> (TestStream/create th/nippy)
                       (test-support/add-elements!
                         (->event* {:user "navy" :team "blue" :score (int 12)} Duration/ZERO)
                         (->event* {:user "scarlet" :team "red" :score (int 3)} Duration/ZERO))
                       (.advanceWatermarkTo (.maxTimestamp window))
                       (test-support/add-elements!
                         (->event* {:user "scarlet" :team "red" :score (int 4)} (mins 2))
                         (->event* {:user "sky" :team "blue" :score (int 3)} Duration/ZERO)
                         (->event* {:user "navy" :team "blue" :score (int 3)} (mins 3)))
                       (.advanceWatermarkTo (-> base-time
                                              (.plus (mins team-window-duration-minutes))))
                       (.advanceWatermarkTo (-> base-time
                                              (.plus (mins allowed-lateness-minutes))
                                              (.plus (mins team-window-duration-minutes))
                                              (.plus (mins 1))))
                       (test-support/add-elements!
                         (->event* {:user "sky" :team "blue" :score (int 3)} (->
                                                                               (mins team-window-duration-minutes)
                                                                               (.minus (secs 5))))
                         (->event* {:user "scarlet" :team "red" :score (int 7)} (mins 4)))
                       (.advanceWatermarkToInfinity))
        team-scores (th/apply! pipeline
                      event-stream
                      (#'game.leader-board/->calculate-team-scores-xf
                        {:team-window-duration team-window-duration-minutes
                         :allowed-lateness allowed-lateness-minutes}))]
    (-> (PAssert/that team-scores)
      (.inWindow window)
      (.containsInAnyOrder ^Iterable [["red" (int 7)]
                                      ["blue" (int 18)]]))
    (-> (PAssert/that team-scores)
      (.inFinalPane window)
      (.empty))
    (test-support/run-test-pipeline! pipeline)))

(deftest test-user-score
  (let [pipeline (test-support/create-test-pipeline)
        event-stream (-> (TestStream/create th/nippy)
                       (test-support/add-elements!
                         (->event* {:user "navy" :team "blue" :score (int 12)} Duration/ZERO)
                         (->event* {:user "scarlet" :team "red" :score (int 3)} Duration/ZERO))
                       (.advanceProcessingTime (mins 7))
                       (test-support/add-elements!
                         (->event* {:user "scarlet" :team "red" :score (int 4)} (mins 2))
                         (->event* {:user "sky" :team "blue" :score (int 3)} Duration/ZERO)
                         (->event* {:user "navy" :team "blue" :score (int 3)} (mins 3)))
                       (.advanceProcessingTime (mins 5))
                       (.advanceWatermarkTo (-> base-time
                                              (.plus (mins allowed-lateness-minutes))
                                              (.plus (hours 12))))
                       (test-support/add-elements!
                         (->event* {:user "scarlet" :team "red" :score (int 3)} (mins 7))
                         (->event* {:user "scarlet" :team "red" :score (int 2)} (->
                                                                                  (mins allowed-lateness-minutes)
                                                                                  (.plus (hours 13)))))
                       (.advanceProcessingTime (mins 6))
                       (test-support/add-elements!
                         (->event* {:user "sky" :team "blue" :score (int 5)} (mins 12)))
                       (.advanceProcessingTime (mins 20))
                       (.advanceWatermarkToInfinity))
        user-scores (th/apply! pipeline
                      event-stream
                      (#'game.leader-board/->calculate-user-scores-xf
                        {:allowed-lateness allowed-lateness-minutes}))]
    (-> (PAssert/that user-scores)
      (.inEarlyGlobalWindowPanes)
      (.containsInAnyOrder ^Iterable [["navy" (int 15)]
                                      ["scarlet" (int 7)]
                                      ["scarlet" (int 12)]
                                      ["sky" (int 3)]
                                      ["sky" (int 8)]]))
    (test-support/run-test-pipeline! pipeline)))