(ns simple.splittable-dofn
  (:require [thurber :as th]
            [clj-time.coerce :as c]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.transforms ParDo DoFn$ProcessContinuation Count Combine)
           (thurber.java TDoFn_Splittable_Unbounded)
           (org.apache.beam.sdk.io.range OffsetRange)
           (org.apache.beam.sdk.transforms.splittabledofn OffsetRangeTracker)
           (org.joda.time Duration Instant)
           (org.apache.beam.sdk.transforms.windowing BoundedWindow Window CalendarWindows AfterWatermark AfterPane)
           (org.apache.beam.sdk Pipeline PipelineResult PipelineResult$State)))

;;
;; Play with Beam windowing in the REPL:
;;
;; => (demo!)                                           ; start a demo
;;
;; => (update-timestamp! #inst "2020-01-01T11:00:00Z")  ; elements will get this timestamp
;; => (put-element! :hello                              ; emits an element into the pipeline
;;
;; => (update-watermark! #inst "2020-01-01T11:00:00Z")  ; next element will update the watermark
;; => (put-element! :world)
;; =>
;; => (cancel-demo!)                                    ; kills the pipeline
;;
;; => (demo!)                                           ; start again from scratch
;;

(defn ^Instant ->instant [ts]
  (if (instance? Instant ts)
    ts (.toInstant (c/to-date-time ts))))

(def ^Duration resume-delay (Duration/millis 250))

(defonce latest-pipeline-result (atom nil))

(defonce elements (atom nil))
(defonce watermark (atom nil))
(defonce timestamp (atom nil))

(defn ^PipelineResult$State get-latest-pipeline-state []
  (when @latest-pipeline-result
    (.getState ^PipelineResult @latest-pipeline-result)))

(defn cancel-demo! []
  (.cancel @latest-pipeline-result))

(defn reset-elements! []
  (reset! elements [])
  (reset! watermark BoundedWindow/TIMESTAMP_MIN_VALUE)
  (reset! timestamp BoundedWindow/TIMESTAMP_MIN_VALUE))

(defn update-timestamp! [ts]
  (reset! timestamp (->instant ts)))

(defn update-watermark! [wm]
  (let [wm-inst (->instant wm)]
    (swap! watermark
      (fn [curr-wm]
        (if (< (compare wm-inst curr-wm) 0)
          (throw (IllegalStateException. "can't regress watermark")) wm-inst)))))

(defn put-element! [elem]
  (swap! elements conj
    {:element elem :watermark @watermark :timestamp @timestamp})
  elem)

;; --

(defn- process-restriction [data_]
  (let [tracker (th/*restriction-tracker)
        ^OffsetRange restriction (.currentRestriction tracker)
        [from to] [(.getFrom restriction) (.getTo restriction)]
        elements-snapshot @elements
        num-elements-snapshot (count elements-snapshot)]
    (loop [curr from]
      (cond
        ;; -- resume scenario/s --
        (>= curr num-elements-snapshot)
        (do
          (.updateWatermark (th/*process-context) @watermark)
          (if (= watermark BoundedWindow/TIMESTAMP_MAX_VALUE)
            (DoFn$ProcessContinuation/stop)
            (-> (DoFn$ProcessContinuation/resume)
              (.withResumeDelay resume-delay))))
        (>= curr to)
        (do
          (-> (DoFn$ProcessContinuation/resume)
            (.withResumeDelay resume-delay)))
        ;; -- output scenario --
        (.tryClaim tracker curr)
        (let [{:keys [element timestamp watermark]} (elements-snapshot curr)]
          (.outputWithTimestamp (th/*process-context) element timestamp)
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

(defn ^Pipeline build-pipeline! [^Pipeline pipeline]
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
  (reset-elements!)
  (let [n-cpu (.availableProcessors (Runtime/getRuntime))
        pipeline (-> {:target-parallelism (max (int (/ n-cpu 2)) (int 1))
                      :block-on-run false}
                   th/create-pipeline)]
    (reset! latest-pipeline-result
      (-> pipeline build-pipeline! (.run)))))