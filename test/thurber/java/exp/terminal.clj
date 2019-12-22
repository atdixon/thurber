(ns thurber.java.exp.terminal
  (:require [clojure.test :refer :all]
            [thurber :as th]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.testing TestPipeline PAssert)
           (org.apache.beam.sdk.transforms Create Values Flatten)
           (org.apache.beam.sdk.values TimestampedValue)
           (org.apache.beam.sdk.transforms.windowing Window)
           (thurber.java.exp TerminalWindowFn TerminalWindow)
           (org.joda.time Duration Instant)
           (org.apache.beam.sdk.schemas.transforms Group)))

(defn- ->terminal-delay [elem]
  (when-let [delay (:terminal-delay elem)] (Duration/millis ^long delay)))

(defn- ->color [elem]
  (:color elem))

(defn- peek* [elem]
  (log/warnf "%s ~ [%s]" elem th/*element-window*) elem)

(deftest test-terminal-window
  (let [p (-> (TestPipeline/create)
            (.enableAbandonedNodeEnforcement true))
        output (th/apply! p
                 (->
                   (Create/timestamped
                     [(TimestampedValue/of {:color "red"} (Instant. 0))
                      (TimestampedValue/of {:color "blue"} (Instant. 9))
                      (TimestampedValue/of {:color "green"} (Instant. 21))
                      (TimestampedValue/of {:color "orange"} (Instant. 26))
                      (TimestampedValue/of {:color "pink" :terminal-delay 15} (Instant. 30))
                      (TimestampedValue/of {:color "purple" :terminal-delay 2} (Instant. 30))])
                   (.withCoder th/nippy))
                 (Window/into
                   (TerminalWindowFn. #'->terminal-delay (Duration/millis 10)))
                 #'->color
                 (Group/globally)
                 (Flatten/iterables)
                 #'peek*)]
    (-> output
      (PAssert/that)
      (.inWindow (TerminalWindow. (Instant. 0) (Instant. 19) false))
      (.containsInAnyOrder ["red" "blue"]))
    (-> output
      (PAssert/that)
      (.inWindow (TerminalWindow. (Instant. 21) (Instant. 33) true))
      (.containsInAnyOrder ["green" "orange" "pink" "purple"]))
    (-> (.run p)
      (.waitUntilFinish))))