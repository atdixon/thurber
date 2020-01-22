(ns thurber.sugar
  (:require [thurber :as th])
  (:import (org.apache.beam.sdk.io TextIO)
           (org.apache.beam.sdk.transforms Count)))

;;;; EXAMPLE ;;;;

;;; see demo/walkthrough.clj

(defn read-text-file [f]
  (.. TextIO (read)
    (from f)))

(defn count-per-element []
  (th/compose
    #'th/->kv
    (.. Count (perKey))
    #'th/kv->clj))

(defn log-sink []
  #'th/log)