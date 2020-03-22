(ns game.game-stats
  (:require [game.user-score]
            [game.hourly-team-score]
            [game.leader-board]
            [thurber :as th]
            [clj-time.format :as f]
            [clj-time.core :as t])
  (:import (org.joda.time Duration)
           (org.apache.beam.sdk.transforms.windowing FixedWindows Window IntervalWindow Sessions)
           (org.apache.beam.examples.common ExampleUtils)
           (org.apache.beam.sdk Pipeline)
           (org.apache.beam.sdk.extensions.gcp.options GcpOptions)
           (org.apache.beam.sdk.transforms Sum Values Mean View Combine)
           (org.apache.beam.sdk.values PCollection KV PCollectionView)
           (com.google.api.services.bigquery.model TableSchema TableRow)))

;; --

(def ^:private score-weight 2.5)

(defn- filter-user-scores [^PCollectionView global-mean-score ^KV user-score]
  (let [score ^Integer (.getValue user-score)
        gmc ^Double (th/*side-input global-mean-score)]
    (> score (* gmc score-weight))))

(defn- ->spammy-users [^PCollection user-scores]
  (let [sum-scores (th/apply!
                     user-scores
                     (Sum/integersPerKey))
        global-mean-score (th/apply!
                            sum-scores
                            (Values/create)
                            (-> (Mean/globally)
                              (.asSingletonView)))]
    (th/apply!
      sum-scores
      (th/filter
        "process-and-filter" #'filter-user-scores global-mean-score))))

(defn- ->spammers-view [fixed-window-duration ^PCollection user-events]
  (let [windowed-user-scores (th/apply!
                               user-events
                               (th/with-name
                                 (Window/into
                                   (FixedWindows/of
                                     (Duration/standardMinutes fixed-window-duration)))
                                 "fixed-windows-user"))
        spammy-users (->spammy-users windowed-user-scores)]
    (th/apply! spammy-users (View/asMap))))

;; --

(def ^:private team-score-row-schema
  (doto (TableSchema.)
    (.setFields
      [(game.leader-board/->table-field-schema "team" "STRING")
       (game.leader-board/->table-field-schema "total_score" "INTEGER")
       (game.leader-board/->table-field-schema "window_start" "STRING")
       (game.leader-board/->table-field-schema "processing_time" "STRING")])))

(defn- ->team-score-row [[k v]]
  (doto (TableRow.)
    (.set "team" k)
    (.set "total_score" v)
    (.set "window_start"
      (->> ^IntervalWindow (th/*element-window) (.start)
        (f/unparse game.hourly-team-score/dt-formatter)))
    (.set "processing_time"
      (f/unparse game.hourly-team-score/dt-formatter (t/now)))))

;; --

(def ^:private session-length-row-schema
  (doto (TableSchema.)
    (.setFields
      [(game.leader-board/->table-field-schema "window_start" "STRING")
       (game.leader-board/->table-field-schema "mean_duration" "FLOAT")])))

(defn- ->session-length-row [[k v]]
  (doto (TableRow.)
    (.set "window_start"
      (->> ^IntervalWindow (th/*element-window) (.start)
        (f/unparse game.hourly-team-score/dt-formatter)))
    (.set "mean_duration" v)))

;; --

(defn- ^Pipeline create-pipeline [opts]
  (let [pipeline (th/create-pipeline opts)
        parsed-opts (.getOptions pipeline)
        custom-conf (th/get-custom-config pipeline)
        gcp-project (-> parsed-opts ^GcpOptions (.as GcpOptions) (.getProject))
        raw-events (th/apply!
                     pipeline
                     (game.leader-board/->game-events-xf
                       (format "projects/%s/topics/%s" gcp-project (:topic custom-conf))))
        user-scores (th/apply!
                      raw-events
                      ;; th/->kv is quite powerful.
                      (th/partial "extract-user-score" #'th/->kv :user :score))
        spammers-view (->spammers-view (:fixed-window-duration custom-conf) user-scores)]

    (th/apply! raw-events
      (th/with-name
        (Window/into
          (FixedWindows/of
            (Duration/standardMinutes (:fixed-window-duration custom-conf))))
        "window-into-fixed-windows")
      (th/filter
        (th/fn* filter-out-spammers [game-event]
          (let [spammers (th/*side-input spammers-view)]
            (contains? spammers (:user game-event)))))
      (game.user-score/->extract-sum-and-score-xf :team)
      (game.leader-board/->write-to-big-query-xf "write-team-sums"
        gcp-project (:dataset custom-conf)
        (str (:game-stats-table-prefix custom-conf) "_team")
        team-score-row-schema #'->team-score-row))

    (th/apply! user-scores
      (th/with-name
        (Window/into
          (Sessions/withGapDuration
            (Duration/standardMinutes (:session-gap custom-conf))))
        "window-into-sessions")
      (Combine/perKey
        (th/combiner
          (th/fn* existential-combine [& _] 0)))
      (th/fn* user-session-info [elem]
        (let [w ^IntervalWindow (th/*element-window)]
          (-> (Duration. (.start w) (.end w))
            (.toPeriod) (.toStandardMinutes) (.getMinutes))))
      (th/with-name
        (Window/into
          (FixedWindows/of
            (Duration/standardMinutes (:user-activity-window-duration custom-conf))))
        "window-to-extract-session-mean")
      (-> (Mean/globally)
        (.withoutDefaults))
      (game.leader-board/->write-to-big-query-xf "write-average-session-length"
        gcp-project (:dataset custom-conf)
        (str (:game-stats-table-prefix custom-conf) "_sessions")
        session-length-row-schema #'->session-length-row))

    pipeline))

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
                         :leaderboard-table-name "leaderboard"
                         :fixed-window-duration 60
                         :session-gap 5
                         :user-activity-window-duration 30
                         :game-stats-table-prefix "game_stats"}})))
        example-utils (ExampleUtils. (.getOptions pipeline))]
    (->> pipeline (.run) (.waitToFinish example-utils))))
