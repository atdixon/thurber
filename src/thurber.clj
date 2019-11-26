(ns thurber
  (:require [camel-snake-kebab.core :as csk]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [taoensso.nippy :as nippy])
  (:import (org.apache.beam.sdk.transforms PTransform Create ParDo GroupByKey DoFn$ProcessContext Count SerializableFunction Combine SerializableBiFunction)
           (java.util Map)
           (thurber.java TDoFn TCoder TOptions TSerializableFunction TProxy TCombine TSerializableBiFunction)
           (org.apache.beam.sdk.values PCollection KV)
           (org.apache.beam.sdk Pipeline)
           (org.apache.beam.sdk.options PipelineOptionsFactory PipelineOptions)
           (clojure.lang MapEntry)
           (org.apache.beam.sdk.transforms.windowing BoundedWindow)
           (org.apache.beam.sdk.coders KvCoder CustomCoder IterableCoder)
           (java.io DataInputStream InputStream DataOutputStream OutputStream)))

;; --

(def ^:private nippy-impl
  (proxy [CustomCoder] []
    (encode [val ^OutputStream out]
      (nippy/freeze-to-out! (DataOutputStream. out) val))
    (decode [^InputStream in]
      (nippy/thaw-from-in! (DataInputStream. in)))))

(def nippy
  (TCoder. #'nippy-impl))

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

;; --

(def ^:dynamic ^PipelineOptions *pipeline-options* nil)
(def ^:dynamic ^DoFn$ProcessContext *process-context* nil)
(def ^:dynamic ^BoundedWindow *element-window* nil)

(def ^:dynamic *proxy-args* nil)

;; --

;; todo consider (try catch) w/ nice reporting around call here
;; todo tagged multi-output [:one <seq> ...]

(defn apply** [fn
               ^PipelineOptions options
               ^DoFn$ProcessContext context
               ^BoundedWindow window
               & args]
  (binding [*pipeline-options* options
            *process-context* context
            *element-window* window]
    (when-let [rv (apply fn (concat args [(.element context)]))]
      (if (seq? rv)
        (doseq [v rv]
          (.output context v))
        (.output context rv)))))

;; --

(defn proxy* [proxy-var & args]
  (TProxy/create proxy-var (into-array Object args)))

;; --

(defn ->beam-args [m]
  (map (fn [[k v]]
         (format "--%s=%s"
           (-> k csk/->camelCase name)
           (cond
             (map? v) (json/write-str v)
             (coll? v) (str/join "," v)
             :else (-> v str (str/escape {\" "\\\""}))))) m))

(defn ^PipelineOptions create-options
  ([]
   (create-options [] TOptions))
  ([opts]
   (create-options opts TOptions))
  ([opts as]
   (-> (PipelineOptionsFactory/fromArgs
         (cond
           (map? opts) (into-array String (->beam-args opts))
           (coll? opts) (into-array String opts)
           :else opts))
     (.as as))))

(defn ^Pipeline create-pipeline
  ([] (Pipeline/create))
  ([opts] (-> (if (instance? PipelineOptions opts)
                opts (create-options opts))
            (Pipeline/create))))

(defn get-custom-config [obj]
  (if (instance? Pipeline obj)
    (recur (.getOptions obj))
    (->> (.getCustomConfig ^TOptions (.as obj TOptions))
      (into {}) walk/keywordize-keys)))

;; --

(defn ^PTransform pardo*
  [xfn & args]
  (ParDo/of (TDoFn. xfn (object-array args))))

(defn- filter-impl [pred-fn & args]
  (when (apply pred-fn args)
    (last args)))

(defn ^PTransform filter* [pred-var & args]
  (apply pardo* #'filter-impl pred-var args))

(defn ^SerializableFunction simple* [fn-var & args]
  (TSerializableFunction. fn-var args))

(defn ^SerializableBiFunction simple-bi* [fn-var & args]
  (TSerializableBiFunction. fn-var args))

;; --

(defn- ^TCoder ->explicit-coder* [prev nxf]
  (when-let [c (:th/coder nxf)]
    (if (= c :th/inherit)
      (.getCoder prev) c)))

(defn- ->normal-form* [xf]
  (cond
    (var? xf) (merge {:th/name (-> xf meta :name name) :th/xform (pardo* xf) :th/coder nippy} (select-keys (meta xf) [:th/name :th/coder]))
    (map? xf) (merge (->normal-form* (:th/xform xf)) (dissoc xf :th/xform))
    (instance? PTransform xf) {:th/xform xf}))

(defn ^PCollection apply!
  "Apply coerce*-able transforms to an input (Pipeline, PCollection, PBegin ...)"
  [input & xfs]
  (reduce
    (fn [acc xf]
      (let [nxf (->normal-form* xf)
            acc' ^PCollection (if (:th/name nxf)
                                (.apply acc (:th/name nxf) (:th/xform nxf))
                                (.apply acc (:th/xform nxf)))
            explicit-coder (->explicit-coder* acc nxf)]
        (when explicit-coder (.setCoder acc' explicit-coder)) acc')) input xfs))

(defn ^PTransform comp* [& [xf-or-name :as xfs]]
  (proxy [PTransform] [(when (string? xf-or-name) xf-or-name)]
    (expand [^PCollection pc]
      (apply apply! pc (if (string? xf-or-name) (rest xfs) xfs)))))

;; --

(defn ^PTransform create* [coll]
  (if (map? coll)
    (-> (Create/of ^Map coll) (.withCoder nippy))
    (-> (Create/of ^Iterable (seq coll)) (.withCoder nippy))))

;; --

(defprotocol CombineFn
  (create-accumulator [this])
  (add-input [this acc input])
  (merge-accumulators [this acc-coll])
  (extract-output [this acc]))

(defmacro def-combiner [& body]
  `(reify CombineFn
     ~@body))

(defn combiner* [xf-var]
  (let [xf (deref xf-var)]
    (cond
      (satisfies? CombineFn xf) (TCombine. xf-var)
      (fn? xf) (simple-bi* xf-var))))

(defn combine-globally [xf-var]
  (Combine/globally (combiner* xf-var)))

(defn combine-per-key [xf-var]
  (Combine/perKey (combiner* xf-var)))

;; --

(defn ^{:th/coder nippy-kv} ->kv
  ([seg]
   (KV/of seg seg))
  ([seg key-fn]
   (KV/of (key-fn seg) seg))
  ([seg key-fn val-fn]
   (KV/of (key-fn seg) (val-fn seg))))

;; --

(defn kv->clj [^KV kv]
  (MapEntry/create (.getKey kv) (.getValue kv)))
