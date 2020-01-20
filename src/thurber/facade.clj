(ns thurber.facade
  (:require [thurber :as th])
  (:import (org.apache.beam.sdk.io TextIO)
           (org.apache.beam.sdk.transforms Count)))

;;;; EXPERIMENTAL ;;;;

(defn read-text-file [f]
  (.. TextIO (read)
    (from f)))

(defn count-per-element []
  (th/compose
    #'th/->kv
    (.. Count (perKey))
    #'th/kv->clj))

(defmacro fn* [& body]
  `(th/inline
     (fn ~@body)))

(defn log-sink []
  #'th/log)