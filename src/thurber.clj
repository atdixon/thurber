(ns thurber
  (:require [camel-snake-kebab.core :as csk]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [taoensso.nippy :as nippy]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.transforms PTransform Create ParDo DoFn$ProcessContext DoFn$OnTimerContext Combine$CombineFn SerializableFunction Filter SerializableBiFunction)
           (java.util Map)
           (thurber.java TDoFn TCoder TOptions TProxy TCombine TDoFn_Stateful TDoFnContext TFn)
           (org.apache.beam.sdk.values PCollection KV PCollectionView TupleTag TupleTagList PCollectionTuple)
           (org.apache.beam.sdk Pipeline PipelineResult)
           (org.apache.beam.sdk.options PipelineOptionsFactory PipelineOptions)
           (clojure.lang MapEntry Keyword IPersistentMap)
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

(defn proxy-with-signature* [proxy-var sig & args]
  (TProxy/create proxy-var sig (into-array Object args)))

(defn proxy* [proxy-var & args]
  (apply proxy-with-signature* proxy-var nil args))

;; --

(defn- ^TFn ->TFn [f]
  (cond
    (instance? TFn f) f
    (var? f) (TFn. f)))

;; --

(defn- var->name [v]
  (or (:th/name (meta v)) (:name (meta v)))
  (-> v meta :name name))

(defn ^PTransform partial
  [fn-like-or-name & args]
  (let [[explicit-name fn- args-]
        (if (string? fn-like-or-name)
          [fn-like-or-name (first args) (rest args)]
          [nil fn-like-or-name args])
        use-name (cond
                   (some? explicit-name) explicit-name
                   (var? fn-) (format "partial:%s" (var->name fn-like-or-name)))]
    (cond-> (-> fn- ->TFn (.partial_ (into-array Object args-)))
      use-name (vary-meta merge {:th/name use-name}))))

(def ser-fn partial)

(defn- kw-impl
  [^Keyword kw elem] (kw elem))

(defn- filter-impl [^TFn pred-fn & args]
  (when (.apply_ pred-fn args) (last args)))

(defn filter [fn-like-or-name & args]
  (let [[explicit-name fn- args-]
        (if (string? fn-like-or-name)
          [fn-like-or-name (first args) (rest args)]
          [nil fn-like-or-name args])
        use-name (cond
                   (some? explicit-name) explicit-name
                   (var? fn-) (format "filter:%s" (var->name fn-like-or-name)))
        tfn- (-> fn- ->TFn)]
    ;; Note: we promote all args from provided fn-like to args passed to filter-impl;
    ;;   these top-level args are used by thurber to infer tags, side-inputs etc so
    ;;   they must be seen at the top level; filter-impl will relay them to the fn-.
    (cond-> (-> #'filter-impl ->TFn
              (.partial_
                (into-array Object
                  (concat [(.withoutPartialArgs tfn-)]
                    args- (.-partialArgs tfn-)))))
      use-name (vary-meta merge {:th/name use-name}))))

;; --

(defmacro inline [fn-form]
  {:pre [(= #'clojure.core/fn (resolve (first fn-form)))
         (symbol? (second fn-form))]}
  (let [name-sym (second fn-form)
        name- (name name-sym)
        [arglists bodies] (if (vector? (nth fn-form 2))
                            [[(nth fn-form 2)] [(drop 3 fn-form)]]
                            [(into [] (map first (nnext fn-form)))
                             (into [] (map rest (nnext fn-form)))])
        lex-syms (->> bodies
                   walk/macroexpand-all
                   (tree-seq coll? seq)
                   (clojure.core/filter simple-symbol?)
                   (clojure.core/filter (set (keys &env)))
                   set vec)
        ;; note: as we are prepending lexical scope symbols even
        ;;  if we have one that is masked by an actual arg sym
        ;;  it will precede and therefore still be masked.
        arglists' (map #(into lex-syms %) arglists)
        fn-form' (list* `fn (map list* arglists' bodies))]
    (intern *ns*
      (with-meta name-sym
        (into {} (map (fn [[k v]] [k (eval v)]))
          (meta name-sym)))
      (eval fn-form'))
    ;; we use raw symbol here so as to not rewrite metadata of symbol
    ;; interned while compiling:
    (if (empty? lex-syms)
      `(intern ~*ns* (symbol ~name-))
      `(thurber/partial (intern ~*ns* (symbol ~name-)) ~@lex-syms))))

(defmacro fn* [& body]
  `(inline
     (fn ~@body)))

;; --

(defn- ^TCoder ->coder [prev nxf]
  (when-let [c (:th/coder nxf)]
    (condp = c
      :th/inherit-or-nippy (or (.getCoder prev) nippy)
      :th/inherit (.getCoder prev)
      c)))

(defn- ->pardo [^TFn xf-fn stateful? ^TFn timer-fn]
  (let [tags (into [] (clojure.core/filter #(instance? TupleTag %) (.-partialArgs xf-fn)))
        views (into [] (clojure.core/filter #(instance? PCollectionView %)) (.-partialArgs xf-fn))
        side-inputs (into [] (clojure.core/filter #(instance? PCollectionView %)) (.-partialArgs xf-fn))]
    (cond-> (ParDo/of (if (or stateful? timer-fn)
                        (TDoFn_Stateful. xf-fn timer-fn)
                        (TDoFn. xf-fn)))
      (not-empty tags)
      (.withOutputTags ^TupleTag (first tags)
        (reduce (fn [^TupleTagList acc ^TupleTag tag]
                  (.and acc tag)) (TupleTagList/empty) (rest tags)))
      (not-empty views)
      (.withSideInputs ^Iterable side-inputs))))

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
     (instance? TFn xf) (let [normal (merge {:th/name (var->name (.-fnVar ^TFn xf)) :th/coder :th/inherit-or-nippy}
                                       (select-keys (meta xf) [:th/name :th/coder :th/timer-fn :th/stateful]) override)]
                          (assoc normal :th/xform (->pardo xf (:th/stateful normal) (->TFn (:th/timer-fn normal)))))
     (instance? PTransform xf) (merge {:th/xform xf} override)
     (keyword? xf) (normalize-xf (partial (str xf) #'kw-impl xf) override)
     (map? xf) (normalize-xf (:th/xform xf) (merge (dissoc xf :th/xform) override)) ;; ...b/c maps may nest.
     (var? xf) (normalize-xf (TFn. xf) override))))

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
   {:pre [(or (var? extractf) (instance? TFn extractf))
          (or (var? combinef) (instance? TFn reducef))
          (or (var? combinef) (instance? TFn reducef))]}
   (TCombine. (->TFn extractf) (->TFn combinef) (->TFn reducef))))

;; --

(defn with-timer [fn-like timer-fn-like]
  {:th/xform fn-like
   :th/timer-fn timer-fn-like})

(defn with-name [xf-like name-]
  {:th/name name-
   :th/xform xf-like})

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