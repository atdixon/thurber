(ns thurber
  (:require [camel-snake-kebab.core :as csk]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [taoensso.nippy :as nippy]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.transforms PTransform Create ParDo DoFn$ProcessContext DoFn$OnTimerContext Combine$CombineFn SerializableFunction Filter SerializableBiFunction)
           (java.util Map)
           (thurber.java TDoFn TCoder TOptions TSerializableFunction TProxy TCombine TSerializableBiFunction TDoFn_Stateful TDoFnContext MutableTransientHolder)
           (org.apache.beam.sdk.values PCollection KV PCollectionView TupleTag TupleTagList PCollectionTuple)
           (org.apache.beam.sdk Pipeline PipelineResult)
           (org.apache.beam.sdk.options PipelineOptionsFactory PipelineOptions)
           (clojure.lang MapEntry Keyword IPersistentMap ITransientCollection)
           (org.apache.beam.sdk.transforms.windowing BoundedWindow)
           (org.apache.beam.sdk.coders KvCoder CustomCoder)
           (java.io DataInputStream InputStream DataOutputStream OutputStream)
           (org.apache.beam.sdk.state ValueState Timer BagState)
           (org.apache.beam.sdk.transforms.splittabledofn RestrictionTracker)
           (org.apache.beam.sdk.transforms.join CoGbkResult CoGbkResultSchema)))

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

(defn ^PipelineResult run-pipeline! [p]
  (if (instance? Pipeline p)
    (.run ^Pipeline p)
    (run-pipeline! (.getPipeline p))))

;; --

(defonce
 ^:private nippy-impl
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

(nippy/extend-freeze
  MutableTransientHolder :thurber/transient-coll
  [val data-output]
  (nippy/freeze-to-out! data-output (.toPersistent ^MutableTransientHolder val)))

(nippy/extend-thaw
  :thurber/transient-coll
  [data-input]
  (MutableTransientHolder. (transient (nippy/thaw-from-in! data-input))))

;; --

;; Clojure thread bindings are more expensive than needed for hot code;
;; ThreadLocals are faster, so we use them for thread bindings instead.

(defonce ^:private ^ThreadLocal tl-context (ThreadLocal.))
(defonce ^:private ^ThreadLocal tl-proxy-args (ThreadLocal.))

(def ^:private get-custom-config-memo
  (let [mem (atom {})]
    (fn [^PipelineOptions opts]
      (if-let [e (find @mem (.getJobName opts))]
        (val e)
        (let [ret (get-custom-config opts)]
          (swap! mem assoc (.getJobName opts) ret)
          ret)))))

(defn ^PipelineOptions *pipeline-options [] (.-pipelineOptions ^TDoFnContext (.get tl-context)))
(defn ^IPersistentMap *custom-config [] (get-custom-config-memo (*pipeline-options)))
(defn ^DoFn$ProcessContext *process-context [] (.-processContext ^TDoFnContext (.get tl-context)))
(defn ^BoundedWindow *element-window [] (.-elementWindow ^TDoFnContext (.get tl-context)))
(defn ^ValueState *value-state [] (.-valueState ^TDoFnContext (.get tl-context)))
(defn ^BagState *bag-state [] (.-bagState ^TDoFnContext (.get tl-context)))
(defn ^Timer *event-timer [] (.-eventTimer ^TDoFnContext (.get tl-context)))
(defn ^DoFn$OnTimerContext *timer-context [] (.-timerContext ^TDoFnContext (.get tl-context)))
(defn ^RestrictionTracker *restriction-tracker [] (.-restrictionTracker ^TDoFnContext (.get tl-context)))

(defn ^"[Ljava.lang.Object;" *proxy-args [] (.get tl-proxy-args))

;; --

(defmacro inline [fn-form]
  {:pre [(= #'clojure.core/fn (resolve (first fn-form)))
         (symbol? (second fn-form))]}
  (let [name-sym (second fn-form)
        name (name name-sym)
        arglists (if (vector? (first (nnext fn-form)))
                   [(first (nnext fn-form))]
                   (into [] (map first (nnext fn-form))))]
    (intern *ns* (with-meta name-sym
                   (merge (into {} (map (fn [[k v]] [k (eval v)]))
                            (meta name-sym)) {:arglists arglists})) (eval fn-form))
    ;; we use raw symbol here so as to not rewrite metadata of symbol
    ;; interned while compiling:
    `(intern ~*ns* (symbol ~name))))

(defmacro fn* [& body]
  `(inline
     (fn ~@body)))

;; --

(defn proxy-with-signature* [proxy-var sig & args]
  (TProxy/create proxy-var sig (into-array Object args)))

(defn proxy* [proxy-var & args]
  (apply proxy-with-signature* proxy-var nil args))

;; --

(defn- var->name [v]
  (or (:th/name (meta v)) (:name (meta v)))
  (-> v meta :name name))

(defn ^PTransform partial
  [fn-var-or-name & args]
  (if (string? fn-var-or-name)
    {:th/name fn-var-or-name
     :th/xform (first args)
     :th/params (rest args)}
    {:th/name (format "partial:%s" (var->name fn-var-or-name))
     :th/xform fn-var-or-name
     :th/params args}))

(defn- kw-impl
  [^Keyword kw elem] (kw elem))

(defn- filter-impl [pred-fn & args]
  (when (apply pred-fn args)
    (last args)))

(defn ^PTransform filter [pred-var-ser-fn-or-name & args]
  (if (string? pred-var-ser-fn-or-name)
    (if (instance? SerializableFunction (first args))
      {:th/name pred-var-ser-fn-or-name
       :th/xform (Filter/by ^SerializableFunction (first args))}
      {:th/name pred-var-ser-fn-or-name
       :th/xform #'filter-impl
       :th/params args})
    (if (instance? SerializableFunction pred-var-ser-fn-or-name)
      {:th/name pred-var-ser-fn-or-name
       :th/xform (Filter/by ^SerializableFunction pred-var-ser-fn-or-name)}
      {:th/name (format "filter:%s" (var->name pred-var-ser-fn-or-name))
       :th/xform #'filter-impl
       :th/params (conj args pred-var-ser-fn-or-name)})))

(defn ser-fn [fn-var & args]
  (case (apply max
          (clojure.core/filter #{1 2}
            (map count (:arglists (meta fn-var)))))
    1 ^SerializableFunction (TSerializableFunction. fn-var args)
    2 ^SerializableBiFunction (TSerializableBiFunction. fn-var args)))

;; --

(defn- ^TCoder ->coder [prev nxf]
  (when-let [c (:th/coder nxf)]
    (condp = c
      :th/inherit-or-nippy (or (.getCoder prev) nippy)
      :th/inherit (.getCoder prev)
      c)))

(defn- ->pardo [xf params timer-params stateful? timer-fn]
  (let [tags (into [] (clojure.core/filter #(instance? TupleTag %) params))
        views (into [] (clojure.core/filter #(instance? PCollectionView %)) params)]
    (cond-> (ParDo/of (if (or stateful? timer-fn)
                        (TDoFn_Stateful. xf timer-fn (object-array params) (object-array timer-params))
                        (TDoFn. xf (object-array params))))
      (not-empty tags)
      (.withOutputTags ^TupleTag (first tags)
                       (reduce (fn [^TupleTagList acc ^TupleTag tag]
                                 (.and acc tag)) (TupleTagList/empty) (rest tags)))
      (not-empty views)
      (.withSideInputs
       ^Iterable (into [] (clojure.core/filter #(instance? PCollectionView %)) params)))))

(defn- set-coder! [pcoll-or-tuple coder]
  (cond
    (instance? PCollection pcoll-or-tuple) (.setCoder ^PCollection pcoll-or-tuple coder)
    (instance? PCollectionTuple pcoll-or-tuple) (do
                                                  (->> ^PCollectionTuple pcoll-or-tuple
                                                    (.getAll)
                                                    (.values)
                                                    (clojure.core/run! #(.setCoder ^PCollection % coder)))
                                                  pcoll-or-tuple)))

(defn- normalize-xf
  ([xf] (normalize-xf xf {}))
  ([xf override]
   (cond
     (instance? PTransform xf) (merge {:th/xform xf} override)
     (keyword? xf) (normalize-xf (partial (str xf) #'kw-impl xf) override)
     (map? xf) (normalize-xf (:th/xform xf) (merge (dissoc xf :th/xform) override)) ;; note: maps may nest.
     (var? xf) (let [normal (merge {:th/name (var->name xf) :th/coder :th/inherit-or-nippy}
                              (select-keys (meta xf) [:th/name :th/coder :th/params :th/timer-fn :th/timer-params
                                                      :th/stateful]) override)]
                 (assoc normal :th/xform (->pardo xf (:th/params normal) (:th/timer-params normal) (:th/stateful normal)
                                           (:th/timer-fn normal)))))))

(defn apply!
  "Apply transforms to an input (Pipeline, PCollection, PBegin ...);
  Answers a PCollection, PCollectionView, or so on ..."
  [input xf-or-prefix & xfs]
  (let [[prefix input xfs']
        (if (string? xf-or-prefix)
          [xf-or-prefix input xfs]
          ["" input (conj xfs xf-or-prefix)])]
    (reduce
      (fn [acc xf]
        (let [nxf (normalize-xf xf)
              ;; Take care here. acc' may commonly be PCollection but can also be
              ;;    PCollectionTuple or PCollectionView, eg.
              acc' (if (:th/name nxf)
                     (.apply acc (str prefix (when (not-empty prefix) ":") (:th/name nxf)) (:th/xform nxf))
                     (.apply acc (str prefix (when (not-empty prefix) ":") (.getName (:th/xform nxf))) (:th/xform nxf)))
              explicit-coder (->coder acc nxf)]
          (when explicit-coder
            (set-coder! acc' explicit-coder)) acc')) input xfs')))

(defn ^PTransform compose [& [xf-or-name :as xfs]]
  (proxy [PTransform] [(when (string? xf-or-name) xf-or-name)]
    (expand [pc]
      (apply apply! pc (if (string? xf-or-name) (rest xfs) xfs)))))

;; --

(defn ^PTransform create
  ([coll] (create nil coll))
  ([name coll]
   (cond->>
     (if (map? coll)
       (-> (Create/of ^Map coll) (.withCoder nippy))
       (-> (Create/of ^Iterable (seq coll)) (.withCoder nippy)))
     name (hash-map :th/name name :th/xform))))

;; --

(defn ^Combine$CombineFn combiner
  ([reducef] (combiner reducef reducef))
  ([combinef reducef] (combiner #'identity combinef reducef))
  ([extractf combinef reducef]
   {:pre [(var? extractf) (var? combinef) (var? reducef)]}
   (TCombine. extractf combinef reducef)))

;; --

(defn ^{:th/coder nippy-kv} ->kv
  ([seg]
   (KV/of seg seg))
  ([key-fn seg]
   (KV/of (key-fn seg) seg))
  ([key-fn val-fn seg]
   (KV/of (key-fn seg) (val-fn seg))))

;; --

(defn ^{:th/coder nippy} kv->clj [^KV kv]
  (MapEntry/create (.getKey kv) (.getValue kv)))

(defn ^{:th/coder nippy-kv} clj->kv [[k v]]
  (KV/of k v))

;; --

(defn log
  ([elem] (log :info elem))
  ([level elem] (log/logp level elem) elem))

(defn log-verbose
  ([elem] (log-verbose :info elem))
  ([level elem] (log/logf level "%s @ %s ∈ %s ∈ %s" elem
                  (.timestamp (*process-context))
                  (.pane (*process-context))
                  (*element-window)) elem))

;; --

(defn co-gbk-result->clj [^CoGbkResult r]
  (let [^CoGbkResultSchema s (-> r .getSchema)
        tags (-> s .getTupleTagList .getAll)]
    (into {}
      (map (fn [^TupleTag tag]
             [(-> tag .getId keyword)
              (->> tag (.getAll r) seq)]) tags))))

(defn kv->clj*
  "Convert a Beam KV *result* to Clojure vector: the result is not serializable and
  rather may contain result/s as lazy sequences backed by Beam lazy Iterable results;
  therefore not for use as DoFn."
  [^KV kv]
  (let [key- (.getKey kv) val- (.getValue kv)]
    (cond
      (instance? CoGbkResult val-) [key- (co-gbk-result->clj val-)]
      (instance? Iterable val-) [key- (seq val-)]
      :else [key- val-])))