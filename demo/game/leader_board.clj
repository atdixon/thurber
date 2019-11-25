(ns game.leader-board
  (:require [game.user-score]
            [game.hourly-team-score]
            [thurber :as th]
            [clj-time.core :as t]
            [clj-time.format :as f])
  (:import (org.joda.time Duration)
           (org.apache.beam.sdk.transforms.windowing FixedWindows Window AfterWatermark AfterProcessingTime GlobalWindows Repeatedly IntervalWindow)
           (org.apache.beam.sdk.io.gcp.pubsub PubsubIO)
           (org.apache.beam.examples.complete.game.utils GameConstants)
           (org.apache.beam.examples.common ExampleUtils)
           (org.apache.beam.sdk Pipeline)
           (org.apache.beam.sdk.io.gcp.bigquery BigQueryIO BigQueryIO$Write$CreateDisposition BigQueryIO$Write$WriteDisposition)
           (com.google.api.services.bigquery.model TableReference TableRow TableSchema TableFieldSchema)
           (org.apache.beam.sdk.extensions.gcp.options GcpOptions)))

(defn- ->table-field-schema [key type]
  (doto (TableFieldSchema.)
    (.setName key) (.setType type)))
;; --

(defn- ->game-events-xf [topic]
  (th/comp* "game-events"
    (-> (PubsubIO/readStrings)
      (.withTimestampAttribute GameConstants/TIMESTAMP_ATTRIBUTE)
      (.fromTopic topic))
    #'game.user-score/parse-event))

;; --

(defn- ->calculate-team-scores-xf [{:keys [team-window-duration allowed-lateness]}]
  (th/comp* "calculate-team-scores"
    {:th/name "leaderboard-team-fixed-windows"
     :th/xform
     (-> (Window/into (FixedWindows/of (Duration/standardMinutes team-window-duration)))
       (.triggering
         (-> (AfterWatermark/pastEndOfWindow)
           (.withEarlyFirings
             (-> (AfterProcessingTime/pastFirstElementInPane)
               (.plusDelayOf (Duration/standardMinutes 5))))
           (.withLateFirings
             (-> (AfterProcessingTime/pastFirstElementInPane)
               (.plusDelayOf (Duration/standardMinutes 10))))))
       (.withAllowedLateness (Duration/standardMinutes allowed-lateness))
       (.accumulatingFiredPanes))}
    (game.user-score/->extract-sum-and-score-xf :team)))

(def ^:private team-score-row-schema
  (doto (TableSchema.)
    (.setFields
      [(->table-field-schema "team" "STRING")
       (->table-field-schema "total_score" "INTEGER")
       (->table-field-schema "window_start" "STRING")
       (->table-field-schema "processing_time" "STRING")
       (->table-field-schema "timing" "STRING")])))

(defn- ->team-score-row [[k v]]
  (doto (TableRow.)
    (.set "team" k)
    (.set "total_score" v)
    (.set "window_start"
      (->> ^IntervalWindow th/*element-window* (.start)
        (f/unparse game.hourly-team-score/dt-formatter)))
    (.set "processing_time"
      (f/unparse game.hourly-team-score/dt-formatter (t/now)))
    (.set "timing"
      (-> th/*process-context* (.pane) (.getTiming) str))))

;; --

(defn- ->calculate-user-scores-xf [{:keys [allowed-lateness]}]
  (th/comp* "calculate-user-scores"
    {:th/name "leaderboard-user-global-windows"
     :th/xform
     (-> (Window/into (GlobalWindows.))
       (.triggering
         (Repeatedly/forever
           (-> (AfterProcessingTime/pastFirstElementInPane)
             (.plusDelayOf (Duration/standardMinutes 10)))))
       (.accumulatingFiredPanes)
       (.withAllowedLateness (Duration/standardMinutes allowed-lateness)))}
    (game.user-score/->extract-sum-and-score-xf :user)))

(def ^:private user-score-row-schema
  (doto (TableSchema.)
    (.setFields
      [(->table-field-schema "user" "STRING")
       (->table-field-schema "total_score" "INTEGER")
       (->table-field-schema "processing_time" "STRING")])))

(defn- ->user-score-row [[k v]]
  (doto (TableRow.)
    (.set "user" k)
    (.set "total_score" v)
    (.set "processing_time"
      (f/unparse game.hourly-team-score/dt-formatter (t/now)))))

;; --

(defn- ->write-to-big-query-xf [xf-name project-id dataset-id table-name
                                ^TableSchema table-row-schema table-row-fn]
  (th/comp* xf-name
    table-row-fn
    (-> (BigQueryIO/writeTableRows)
      (.to (doto (TableReference.)
             (.setProjectId project-id)
             (.setDatasetId dataset-id)
             (.setTableId table-name)))
      (.withSchema table-row-schema)
      (.withCreateDisposition BigQueryIO$Write$CreateDisposition/CREATE_IF_NEEDED)
      (.withWriteDisposition BigQueryIO$Write$WriteDisposition/WRITE_APPEND))))

;; --

(defn- ^Pipeline create-pipeline [opts]
  (let [pipeline (th/create-pipeline opts)
        parsed-opts (.getOptions pipeline)
        custom-conf (th/get-custom-config pipeline)
        gcp-project (-> parsed-opts ^GcpOptions (.as GcpOptions) (.getProject))
        game-events (th/apply! pipeline
                      (->game-events-xf (format "projects/%s/topics/%s" gcp-project (:topic custom-conf))))]
    (doto pipeline
      (th/apply!
        game-events
        (->calculate-team-scores-xf custom-conf)
        (->write-to-big-query-xf "write-team-score-sums"
          gcp-project (:dataset custom-conf)
          (str (:leaderboard-table-name custom-conf) "_team")
          team-score-row-schema #'->team-score-row))
      (th/apply!
        game-events
        (->calculate-user-scores-xf custom-conf)
        (->write-to-big-query-xf "write-user-score-sums"
          gcp-project (:dataset custom-conf)
          (str (:leaderboard-table-name custom-conf) "_user")
          user-score-row-schema #'->user-score-row)))))

(defn demo! [& args]
  (let [pipeline (create-pipeline
                   (concat
                     args
                     (th/->beam-args
                       {:streaming true
                        :custom-config
                        {:dataset "thurber_demo_game"
                         :topic "thurber-demo-game"
                         :team-window-duration 60
                         :allowed-lateness 120
                         :leaderboard-table-name "leaderboard"}})))
        example-utils (ExampleUtils. (.getOptions pipeline))]
    (->> pipeline (.run) (.waitToFinish example-utils))))
