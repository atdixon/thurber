(ns thurber
  (:require [camel-snake-kebab.core :as csk]
            [thurber.coder :as coder]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.walk :as walk])
  (:import (org.apache.beam.sdk.transforms PTransform Create ParDo GroupByKey DoFn$ProcessContext)
           (java.util Map)
           (thurber.java TDoFn TCoder TOptions)
           (org.apache.beam.sdk.values PCollection KV)
           (org.apache.beam.sdk Pipeline)
           (org.apache.beam.sdk.options PipelineOptionsFactory PipelineOptions)
           (clojure.lang MapEntry)
           (org.apache.beam.sdk.transforms.windowing BoundedWindow)))

;; --

(def ^:dynamic ^PipelineOptions *pipeline-options* nil)
(def ^:dynamic ^DoFn$ProcessContext *process-context* nil)
(def ^:dynamic ^BoundedWindow *element-window* nil)

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

(defn ^PTransform partial*
  [xfn & args]
  (ParDo/of (TDoFn. xfn (object-array args))))

(defn- ^TCoder ->explicit-coder* [prev xf]
  (cond
    (var? xf) coder/nippy
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
    (.withCoder coder/nippy)))

;; --
