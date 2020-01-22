(ns game.stateful-team-score
  (:require [thurber :as th]
            [game.hourly-team-score]
            [game.leader-board]
            [clj-time.core :as t]
            [clj-time.format :as f])
  (:import (org.apache.beam.examples.common ExampleUtils)
           (org.apache.beam.sdk Pipeline)
           (org.apache.beam.sdk.extensions.gcp.options GcpOptions)
           (com.google.api.services.bigquery.model TableSchema TableRow)
           (org.apache.beam.sdk.transforms.windowing IntervalWindow)
           (org.apache.beam.sdk.values KV)))

;; --

(def ^:private team-score-row-schema
  (doto (TableSchema.)
    (.setFields
      [(game.leader-board/->table-field-schema "team" "STRING")
       (game.leader-board/->table-field-schema "total_score" "INTEGER")
       (game.leader-board/->table-field-schema "processing_time" "STRING")])))

(defn- ->team-score-row [[k v]]
  (doto (TableRow.)
    (.set "team" k)
    (.set "total_score" v)
    (.set "processing_time"
      (f/unparse game.hourly-team-score/dt-formatter (t/now)))))

;; --

(defn- ^:th/stateful update-team-score [threshold-score ^KV elem]
  (let [old-total-score (or (.read (th/*value-state)) 0)
        new-total-score (+ old-total-score (:score (.getValue elem)))]
    (.write (th/*value-state) new-total-score)
    (when (< (quot old-total-score threshold-score) (quot (.read (th/*value-state)) threshold-score))
      (KV/of (.getKey elem) (.read (th/*value-state))))))

;; --

(defn- ^Pipeline create-pipeline [opts]
  (let [pipeline (th/create-pipeline opts)
        parsed-opts (.getOptions pipeline)
        custom-conf (th/get-custom-config pipeline)
        gcp-project (-> parsed-opts ^GcpOptions (.as GcpOptions) (.getProject))]
    (doto pipeline
      (th/apply!
        (game.leader-board/->game-events-xf
          (format "projects/%s/topics/%s" gcp-project (:topic custom-conf)))
        (th/partial "map-team-as-key" #'th/->kv :team)
        (th/partial #'update-team-score (:threshold-score custom-conf))
        (game.leader-board/->write-to-big-query-xf "write-team-leaders"
          gcp-project (:dataset custom-conf)
          (str (:leaderboard-table-name custom-conf) "_team_leader")
          team-score-row-schema #'->team-score-row)))))

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
                         :threshold-score 5000}})))
        example-utils (ExampleUtils. (.getOptions pipeline))]
    (->> pipeline (.run) (.waitToFinish example-utils))))
