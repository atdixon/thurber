(ns game.user-score
  (:require [thurber :as th]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.io TextIO)
           (org.apache.beam.sdk.values KV)
           (org.apache.beam.sdk.transforms Sum)))

(defn- parse-event [elem]
  (try
    (let [[user team score ts :as parts] (map str/trim (str/split elem #","))]
      (if (>= (count parts) 4)
        {:user user :team team :score (Integer/parseInt score) :timestamp (Long/parseLong ts)}
        (log/warnf "parse error on %s, missing part" elem)))
    (catch NumberFormatException e
      (log/warnf "parse error on %s, %s" elem (.getMessage e)))))

(defn- ^{:th/coder th/nippy-kv} ->field-and-score-kv [field elem]
  (KV/of (field elem) (:score elem)))

(defn ->extract-sum-and-score-xf [field]
  (th/compose "extract-sum-and-score"
    (th/partial #'->field-and-score-kv field)
    (Sum/integersPerKey)
    ;; It is not necessary to convert Beam's KV type to Clojure; however
    ;; doing so allows us to employ Clojure destructuring downstream.
    ;; Beam KVs become MapEntrys here; these can be destructured like
    ;; vectors. See format-row.
    #'th/kv->clj))

(defn- format-row [[k v]]
  (format "user: %s, total_score: %d" k v))

(defn- ->write-to-text-xf [output row-formatter]
  (th/compose "write-to-text"
    row-formatter
    (-> (TextIO/write)
      (.to ^String output))))

(defn- create-pipeline [opts]
  (let [pipeline (th/create-pipeline opts)
        conf (th/get-custom-config pipeline)]
    (doto pipeline
      (th/apply!
        (-> (TextIO/read)
          (.from ^String (:input conf)))
        #'parse-event
        (->extract-sum-and-score-xf :user)
        (->write-to-text-xf (:output conf) #'format-row)))))

(defn demo! [& args]
  (-> (create-pipeline
        (concat
          args
          (th/->beam-args
            {:custom-config
             {:input "gs://apache-beam-samples/game/gaming_data*.csv"
              :output "gs://thurber-demo/user-score-"}})))
    (.run)))
