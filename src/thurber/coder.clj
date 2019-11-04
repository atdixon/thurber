(ns thurber.coder
  (:require [taoensso.nippy :as nippy])
  (:import (java.io DataOutputStream DataInputStream InputStream OutputStream)
           (thurber.java TCoder)
           (org.apache.beam.sdk.coders KvCoder)
           (clojure.lang MapEntry)))

(defn- nippy-encode* [val ^OutputStream out]
  (nippy/freeze-to-out! (DataOutputStream. out) val))

(defn- nippy-decode* [^InputStream in]
  (nippy/thaw-from-in! (DataInputStream. in)))

(def nippy
  (TCoder. #'nippy-encode* #'nippy-decode*))

(def nippy-kv (KvCoder/of nippy nippy))

;; nippy codes MapEntry as vectors by default; but we want them to stay
;; MapEntry after thaw:

(nippy/extend-freeze
 MapEntry :thurber/map-entry
 [val data-output]
 (let [[k v] val]
   (nippy/freeze-to-out! data-output [k v])))

(nippy/extend-thaw
 :thurber/map-entry
 [data-input]
 (let [[k v] (nippy/thaw-from-in! data-input)]
   (MapEntry/create k v)))
