(ns thurber-xfs
  (:require [thurber :as th]
            [thurber.coder :as coder])
  (:import (org.apache.beam.sdk.transforms GroupByKey Count)
           (org.apache.beam.sdk.values KV)
           (clojure.lang MapEntry)))

(defn- ->kv [seg]
  (KV/of seg nil))

(defn- kv->me [^KV kv]
  (MapEntry/create (.getKey kv) (.getValue kv)))

;; --

(defn peek- [seg]
  (locking ->kv (println "seggy" seg))
  seg)

(def count-per-key
  (th/comp* "count-per-key"
            {::th/xf #'->kv
             ::th/coder coder/nippy-kv}
            (Count/perKey)
            #'kv->me))
