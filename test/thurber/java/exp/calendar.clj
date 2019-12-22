(ns thurber.java.exp.calendar
  (:require [clojure.test :refer :all]
            [thurber :as th]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.testing TestPipeline PAssert)
           (org.apache.beam.sdk.transforms Create)
           (org.apache.beam.sdk.transforms.windowing Window IntervalWindow)
           (thurber.java.exp CalendarDayWindowFn)
           (org.joda.time DateTimeZone DateTime Instant)
           (org.apache.beam.sdk.values TimestampedValue)))

(defn- ^Instant dt->instant*
  ([tz-id ^Integer year ^Integer month ^Integer day ^Integer hour ^Integer minute]
   (dt->instant* tz-id year month day hour minute 0 0))
  ([tz-id ^Integer year ^Integer month ^Integer day ^Integer hour ^Integer minute ^Integer second ^Integer millis]
   (-> (DateTime. year month day hour minute second millis)
     (.withZoneRetainFields
       (DateTimeZone/forID tz-id))
     (.withZone (DateTimeZone/forID "UTC"))
     (.toInstant))))

(defn- ->tz* [elem]
  (DateTimeZone/forID ^String (:tz elem)))

(defn- pick* [elem]
  (log/infof "%s ~ [%s]" elem th/*element-window*)
  (:color elem))

(deftest test-count-words
  (let [p (-> (TestPipeline/create)
            (.enableAbandonedNodeEnforcement true))
        output (th/apply! p
                 (->
                   (Create/timestamped
                     [(TimestampedValue/of {:tz "America/Chicago" :color "red"} (dt->instant* "America/Chicago" 2020 12 20 0 0))
                      (TimestampedValue/of {:tz "America/Chicago" :color "blue"} (dt->instant* "America/Chicago" 2020 12 20 10 30))
                      (TimestampedValue/of {:tz "America/Los_Angeles" :color "green"} (dt->instant* "America/Chicago" 2020 12 20 0 30))])
                   (.withCoder th/nippy))
                 (Window/into
                   (CalendarDayWindowFn. #'->tz*))
                 #'pick*)]
    (-> output
      (PAssert/that)
      (.inWindow (IntervalWindow.
                   (dt->instant* "America/Chicago" 2020 12 20 0 0)
                   (dt->instant* "America/Chicago" 2020 12 21 0 0)))
      (.containsInAnyOrder ["red" "blue"]))
    (-> output
      (PAssert/that)
      (.inWindow (IntervalWindow.
                   (dt->instant* "America/Los_Angeles" 2020 12 19 0 0)
                   (dt->instant* "America/Los_Angeles" 2020 12 20 0 0)))
      (.containsInAnyOrder ["green"]))
    (-> (.run p)
      (.waitUntilFinish))))