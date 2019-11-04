(ns thurber
  (:require [camel-snake-kebab.core :as csk]
            [thurber.coder :as coder]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.walk :as walk])
  (:import (org.apache.beam.sdk.transforms PTransform Create ParDo GroupByKey)
           (java.util Map)
           (thurber.java TDoFn TCoder TOptions)
           (org.apache.beam.sdk.values PCollection)
           (org.apache.beam.sdk Pipeline)
           (org.apache.beam.sdk.options PipelineOptionsFactory PipelineOptions)))

(defn- coerce-args [args]
  (into-array
   String (if (map? args)
            (map (fn [[k v]]
                   (format "--%s=%s"
                           (-> k csk/->camelCase name)
                           (if (or (map? v) (coll? v))
                             (json/write-str v)
                             (-> v str (str/escape {\" "\"\""}))))) args)
            args)))

(defn ^PipelineOptions create-opts*
  ([]
   (create-opts* [] TOptions))
  ([args]
   (create-opts* args TOptions))
  ([args as]
   (-> (PipelineOptionsFactory/fromArgs
        (coerce-args args))
       (.as as))))

(defn get-config* [obj]
  (if (instance? Pipeline obj)
    (recur (.getOptions obj))
    (->> (.getCustomConfig ^TOptions (.as obj TOptions))
         (into {}) walk/keywordize-keys)))

;; --

(defn ^PTransform coerce* [xf]
  (cond
    (var? xf) (cond
                (fn? @xf) (ParDo/of (TDoFn. xf))
                ; note: this is cute, allowing user to specify var holding a
                ;       PTransform ... but let's not, let's make the user have
                ;       to learn a thing.
                #_#_(instance? PTransform @xf) @xf
                :else (throw (ex-info "bad xf" {:unexpected-xf xf})))
    (map? xf) (recur (:thurber/xf xf))
    :else (if (instance? PTransform xf)
            xf (throw (ex-info "bad xf" {:unexpected-xf xf})))))

(defn ^PTransform partial*
  [xfn & args]
  (ParDo/of (TDoFn. xfn (object-array args))))

(defn- ^TCoder ->explicit-coder* [prev xf]
  (cond
    (var? xf) coder/nippy
    (map? xf) (let [c (:thurber/coder xf)]
                (if (= c :thurber/inherit)
                  (.getCoder prev) c))
    :else nil))

(defn ^PCollection apply!
  "Apply coerce*-able transforms to an input (Pipeline, PCollection, PBegin ...)"
  [input & xfs]
  (reduce
   (fn [acc xf]
     (let [acc' ^PCollection (.apply acc (coerce* xf))
           explicit-coder (->explicit-coder* acc xf)]
       (when explicit-coder (.setCoder acc' explicit-coder)) acc')) input xfs))

(defn ^PTransform comp* [comp-name & xfs]
  (proxy [PTransform] [comp-name]
    (expand [^PCollection pc]
      (apply apply! pc xfs))))

;; --

(defn ^PTransform create* [coll]
  (->
    (if (map? coll)
      (Create/of ^Map coll)
      (Create/of ^Iterable (seq coll)))
    (.withCoder coder/nippy)))

;; --

;; --
