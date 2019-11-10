(ns thurber.xform
  (:require [thurber :as th]
            [thurber.coder :as coder])
  (:import (org.apache.beam.sdk.transforms Count)
           (org.apache.beam.sdk.values KV)
           (clojure.lang MapEntry)))

(defn ->kv
  ([seg]
   (KV/of seg seg))
  ([seg key-fn]
   (KV/of (key-fn seg) seg))
  ([seg key-fn val-fn]
   (KV/of (key-fn seg) (val-fn seg))))

;; --

(defn kv->clj [^KV kv]
  (MapEntry/create (.getKey kv) (.getValue kv)))

;; --

(def count-per-key
  (th/comp* "count-per-key"
            {:th/xform #'->kv
             :th/coder coder/nippy-kv}
            (Count/perKey)
            #'kv->clj))
