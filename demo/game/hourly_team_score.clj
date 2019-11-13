(ns game.hourly-team-score
  (:require [game.user-score]
            [thurber :as th]
            [clj-time.format :as f]
            [clj-time.coerce :as c]
            [clj-time.core :as t])
  (:import (org.apache.beam.sdk.io TextIO FileBasedSink$FilenamePolicy FileBasedSink$OutputFileHints FileBasedSink)
           (org.joda.time Instant Duration)
           (org.apache.beam.sdk.transforms WithTimestamps)
           (org.apache.beam.sdk.transforms.windowing Window FixedWindows IntervalWindow BoundedWindow PaneInfo)
           (org.apache.beam.sdk.io.fs ResourceId ResolveOptions$StandardResolveOptions)
           (thurber.java.exp TFilenamePolicy)))

(def min-parser
  (f/formatter "yyyy-MM-dd-HH-mm" (t/time-zone-for-id "America/Los_Angeles")))

(def dt-formatter
  (f/formatter "yyyy-MM-dd HH:mm:ss.SSS" (t/time-zone-for-id "America/Los_Angeles")))

(defn- filter-start-time [min-start elem]
  (> (:timestamp elem) (c/to-long min-start)))

(defn- filter-end-time [max-end elem]
  (< (:timestamp elem) (c/to-long max-end)))

(defn- ->timestamp [elem]
  (-> elem :timestamp (Instant.)))

(defn- format-row [[k v]]
  (format "team: %s, total_score: %d, window_start: %s" k v
    (->> ^IntervalWindow th/*element-window* (.start) (f/unparse dt-formatter))))

(defn- filename-prefix-for-window [^ResourceId prefix ^IntervalWindow window]
  (format "%s-%s-%s" (if (.isDirectory prefix) "" (.getFilename prefix))
    (f/unparse dt-formatter (.start window)) (f/unparse dt-formatter (.end window))))

(def per-window-files
  (proxy [FileBasedSink$FilenamePolicy] []
    (windowedFilename [shard-number num-shards ^BoundedWindow window
                       ^PaneInfo pane-info ^FileBasedSink$OutputFileHints output-file-hints]
      (let [prefix ^ResourceId th/*proxy-config*
            filename (format "%s-%s-of-%s%s" (filename-prefix-for-window prefix window)
                       shard-number num-shards (.getSuggestedFilenameSuffix output-file-hints))]
        (-> prefix (.getCurrentDirectory)
          (.resolve filename ResolveOptions$StandardResolveOptions/RESOLVE_FILE))))))

(defn- ->write-to-text-xf [output row-formatter]
  (let [resource (FileBasedSink/convertToFileResourceIfPossible output)]
    (th/comp* "write-to-text"
      row-formatter
      (-> (TextIO/write)
        (.to (TFilenamePolicy. resource #'per-window-files))
        (.withTempDirectory (.getCurrentDirectory resource))
        (.withWindowedWrites)
        (.withNumShards 3)))))

(defn- create-pipeline [opts]
  (let [pipeline (th/create-pipeline opts)
        conf (th/get-custom-config pipeline)]
    (doto pipeline
      (th/apply!
        (-> (TextIO/read)
          (.from ^String (:input conf)))
        #'game.user-score/parse-event
        ;; Use filter* w/ optional partial arguments to filter elements per a
        ;; predicate fn.
        (th/filter* #'filter-start-time (f/parse min-parser (:start-min conf)))
        (th/filter* #'filter-end-time (f/parse min-parser (:stop-min conf)))
        ;; Some Beam functions we can use directly by converting a Clojure function
        ;; to a simple* Beam function.
        (WithTimestamps/of (th/simple* #'->timestamp))
        (Window/into
          (FixedWindows/of
            (Duration/standardMinutes (:window-duration conf))))
        (game.user-score/->extract-sum-and-score-xf :team)
        (#'->write-to-text-xf (:output conf) #'format-row)))))

(defn demo! [& args]
  (-> (create-pipeline
        (concat
          args
          (th/->beam-args
            {:custom-config
             {:start-min "1970-01-01-00-00"
              :stop-min "2100-01-01-00-00"
              :window-duration 60
              :input "gs://apache-beam-samples/game/gaming_data*.csv"
              :output "gs://thurber-demo/hourly-team-score-"}})))
    (.run)))