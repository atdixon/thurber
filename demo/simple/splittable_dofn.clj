(ns simple.splittable-dofn
  (:require [thurber :as th]
            [clj-time.coerce :as c]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.transforms ParDo DoFn$ProcessContinuation Count Combine)
           (thurber.java TDoFn_Splittable_Unbounded)
           (org.apache.beam.sdk.io.range OffsetRange)
           (org.apache.beam.sdk.transforms.splittabledofn OffsetRangeTracker)
           (org.joda.time Duration DateTimeZone)
           (org.apache.beam.sdk.transforms.windowing BoundedWindow Window CalendarWindows AfterWatermark AfterPane)
           (org.apache.beam.sdk Pipeline PipelineResult PipelineResult$State)))

;;
;; Play with Beam windowing in the REPL:
;;
;; => (demo!)                                           ; start a demo
;;
;; => (update-timestamp! #inst "2020-01-01T11:00:00Z")  ; elements will get this timestamp
;; => (put-data! :hello)                                ; emits an element into the pipeline
;;
;; => (update-watermark! #inst "2020-01-01T11:00:00Z")  ; next element will update the watermark
;; => (put-data! :world)
;; =>
;; => (.cancel @latest-pipeline-result)                 ; kills the pipeline
;; => (demo!)                                           ; start again from scratch
;;

(defonce latest-pipeline-result (atom nil))

(defonce data (atom nil))
(defonce watermark (atom nil))
(defonce timestamp (atom nil))

(defn- get-latest-pipeline-state []
  (when @latest-pipeline-result
    (.getState ^PipelineResult @latest-pipeline-result)))

(defn- reset-data! []
  (reset! data [])
  (reset! watermark BoundedWindow/TIMESTAMP_MIN_VALUE)
  (reset! timestamp BoundedWindow/TIMESTAMP_MIN_VALUE))

(defn- update-timestamp! [ts]
  (reset! timestamp
    (.toInstant (c/to-date-time ts))))

(defn- update-watermark! [wm]
  (reset! watermark
    (.toInstant (c/to-date-time wm))))

(defn- put-data! [elem]
  (swap! data conj
    {:data elem :watermark @watermark :timestamp @timestamp}) elem)

;; --

(defn- process-restriction [data_]
  (let [tracker (th/*restriction-tracker)
        ^OffsetRange restriction (.currentRestriction tracker)
        [from to] [(.getFrom restriction) (.getTo restriction)]
        data-snapshot @data]
    (loop [curr from]
      (cond
        ;; -- resume scenario --
        (or (>= curr to)
          (>= curr (count data-snapshot)))
        (-> (DoFn$ProcessContinuation/resume)
          (.withResumeDelay (Duration/millis 1000)))
        ;; -- output scenario --
        (.tryClaim tracker curr)
        (let [{:keys [data timestamp watermark]} (data-snapshot curr)]
          (.outputWithTimestamp (th/*process-context) data timestamp)
          (.updateWatermark (th/*process-context) watermark)
          (recur (inc curr)))
        ;; -- stop scenario --
        :else
        (DoFn$ProcessContinuation/stop)))))

(defn- init-restriction [data_]
  (OffsetRange. 0 Long/MAX_VALUE))

(defn- get-tracker [restriction]
  (OffsetRangeTracker. restriction))

(def thingy-xf
  (ParDo/of
    (TDoFn_Splittable_Unbounded. #'process-restriction #'init-restriction, #'get-tracker)))

(defn- ^Pipeline build-pipeline! [^Pipeline pipeline]
  (doto pipeline
    (th/apply!
      (th/create [:kickoff])
      {:th/xform thingy-xf
       :th/coder th/nippy}
      (-> (Window/into
            (CalendarWindows/days 1))
        (.triggering
          (-> (AfterWatermark/pastEndOfWindow)
            (.withEarlyFirings
              (AfterPane/elementCountAtLeast 1))
            (.withLateFirings
              (AfterPane/elementCountAtLeast 1))))
        (.withAllowedLateness (Duration/standardDays 7))
        (.accumulatingFiredPanes))
      (-> (Combine/globally
            (Count/combineFn))
        (.withoutDefaults))
      #'th/log-verbose)))

(defn demo! []
  (when (= PipelineResult$State/RUNNING (get-latest-pipeline-state))
    (throw (RuntimeException. "already running")))
  (reset-data!)
  (let [n-cpu (.availableProcessors (Runtime/getRuntime))
        pipeline (-> {:target-parallelism (max (int (/ n-cpu 2)) (int 1))
                      :block-on-run false}
                   th/create-pipeline)]
    (reset! latest-pipeline-result
      (-> pipeline build-pipeline! (.run)))))