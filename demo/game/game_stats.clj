(ns game.game-stats
  (:require [game.user-score]
            [game.hourly-team-score]
            [game.leader-board]
            [thurber :as th])
  (:import (org.joda.time Duration)
           (org.apache.beam.sdk.transforms.windowing FixedWindows Window)
           (org.apache.beam.examples.common ExampleUtils)
           (org.apache.beam.sdk Pipeline)
           (org.apache.beam.sdk.extensions.gcp.options GcpOptions)
           (org.apache.beam.sdk.transforms Sum Values Mean View)
           (org.apache.beam.sdk.values PCollection KV)))

;; --

(def ^:private score-weight 2.5)

(defn- filter-user-scores [global-mean-score ^KV user-score]
  (let [score ^Integer (.getValue user-score)
        gmc ^Double (.sideInput (th/*process-context*) global-mean-score)]
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
     (th/filter*
      "process-and-filter" #'filter-user-scores global-mean-score))))

(defn- ->spammers-view [fixed-window-duration ^PCollection user-events]
  (let [windowed-user-scores (th/apply!
                              user-events
                              {:th/name "fixed-windows-user"
                               :th/xform
                               (Window/into
                                (FixedWindows/of
                                 (Duration/standardMinutes fixed-window-duration)))})
        spammy-users (->spammy-users windowed-user-scores)]
    (th/apply! spammy-users (View/asMap))))

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
                     (th/partial* "extract-user-score" #'th/->kv :user :score))
        spammers-view (->spammers-view (:fixed-window-duration custom-conf) user-scores)]
    (doto pipeline
      (th/apply!
       ;; todo !!!
       ))))

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
