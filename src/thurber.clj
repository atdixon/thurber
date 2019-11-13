(ns thurber
  (:require [camel-snake-kebab.core :as csk]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [taoensso.nippy :as nippy])
  (:import (org.apache.beam.sdk.transforms PTransform Create ParDo GroupByKey DoFn$ProcessContext Count SerializableFunction)
           (java.util Map)
           (thurber.java TDoFn TCoder TOptions TSerializableFunction)
           (org.apache.beam.sdk.values PCollection KV)
           (org.apache.beam.sdk Pipeline)
           (org.apache.beam.sdk.options PipelineOptionsFactory PipelineOptions)
           (clojure.lang MapEntry)
           (org.apache.beam.sdk.transforms.windowing BoundedWindow)
           (org.apache.beam.sdk.coders KvCoder CustomCoder)
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

(def ^:dynamic *proxy-config* nil)

;; --

(defn- output-one* [val]
  (if
    ;; These are the segment types that we allow to flow.
    (or (map? val)
      (instance? MapEntry val)
      (instance? KV val)
      (number? val)
      (string? val))
    (.output *process-context* val)
    (throw (RuntimeException. (format "invalid output: %s/%s" val (type val))))))

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
          (output-one* v))
        (output-one* rv)))))

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

(defn create-pipeline [opts]
  (-> (if (instance? PipelineOptions opts)
        opts (create-options opts))
    (Pipeline/create)))

(defn get-custom-config [obj]
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
    (map? xf) (recur (:th/xform xf))
    :else (if (instance? PTransform xf)
            xf (throw (ex-info "bad xf" {:unexpected-xf xf})))))

(defn- ^TCoder ->explicit-coder* [prev xf]
  (cond
    (var? xf) nippy
    (map? xf) (let [c (:th/coder xf)]
                (if (= c :th/inherit)
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
    (.withCoder nippy)))

;; --

(defn ^PTransform partial*
  [xfn & args]
  (ParDo/of (TDoFn. xfn (object-array args))))

(defn- filter-impl [pred-fn & args]
  (when (apply pred-fn args)
    (last args)))

(defn ^PTransform filter* [pred-fn & args]
  (apply partial* #'filter-impl pred-fn args))

(defn ^SerializableFunction simple* [fn & args]
  (TSerializableFunction. fn args))

;; --

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
