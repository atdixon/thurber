(ns word-count.basic
  (:require [thurber :as th]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.io TextIO)
           (org.apache.beam.sdk.transforms Count)))

;; Simple Clojure functions can serve as Beam DoFns.
;;
;; When such a function evaluates to a Clojure sequence, each value within the
;; sequence is output downstream as an element.
;;
;; By using lazy Clojure sequences, we can produce many elements
;; with minimal memory consumption.
(defn- extract-words [sentence]
  (remove empty? (str/split sentence #"[^\p{L}]+")))

;; When a function evaluates to a simple single value like a String,
;; this single value is emitted downstream.
(defn- format-as-text [[k v]]
  (format "%s: %d" k v))

(defn- sink* [elem]
  (log/info elem))

;; A reusable transform.
(def count-words-xf
  (th/comp*
    "count-words"
    #'extract-words
    {:th/xform #'th/->kv
     :th/coder th/nippy-kv}
    (Count/perKey)
    ;; Not necessary to convert to KV to clj (ie MapEntry)
    ;; but this allows downstream to use Clojure destructuring.
    #'th/kv->clj))

(defn- create-pipeline [opts]
  (let [pipeline (th/create-pipeline opts)
        conf (th/get-custom-config pipeline)]
    (doto pipeline
      (th/apply!
       (-> (TextIO/read)
           (.from ^String (:input-file conf)))
       count-words-xf
       #'format-as-text
       #'sink*))))

(defn demo! []
  (-> (create-pipeline
       ;; Thurber fully supports Beam's PipelineOptions and static Java interfaces.
       ;;
       ;; Thurber also supports Clojure/EDN maps for providing options; core Beam
       ;; options are provided by their standard names (as skeleton case); config
       ;; unique to your pipeline can be specified under :custom-config.
       ;;
       ;; Config provided this way must be serializable to JSON (per Beam).
       {:target-parallelism 25
        :custom-config {:input-file "demo/word_count/lorem.txt"}})
      (.run)))
