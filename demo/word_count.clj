(ns word-count
  (:require [thurber :as th]
            [thurber-xfs :as xfs]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.io TextIO)
           (org.apache.beam.sdk Pipeline)))

(defn- extract-words [sentence]
  (remove empty? (str/split sentence #"[^\p{L}]+")))

(defn- format-as-text [[k v]]
  (format "%s: %d" k v))

(defn- sink* [seg]
  (log/info seg))

(defn- create-pipeline [opts]
  (let [pipeline (-> opts th/create-opts* Pipeline/create)
        conf (th/get-config* pipeline)]
    (doto pipeline
      (th/apply!
       (-> (TextIO/read)
           (.from (:input-file conf)))
       (th/comp*
        "count-words"
        #'extract-words
        xfs/count-per-key)
       #'format-as-text
       #'sink*))))

(defn run* []
  (-> (create-pipeline
       {:target-parallelism 25
        :custom-config {:input-file "project.clj"}})
      (.run)))
