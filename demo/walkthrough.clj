(ns walkthrough
  (:require [thurber :as th]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.io TextIO)
           (org.apache.beam.sdk.transforms Count Combine GroupByKey WithTimestamps WithKeys View Mean FlatMapElements SerializableFunction)
           (org.apache.beam.sdk.coders VarLongCoder)
           (org.apache.beam.sdk.values KV PCollectionView TypeDescriptors)
           (org.apache.beam.sdk.transforms.windowing PaneInfo$Timing)
           (org.joda.time Duration Instant)))

;;;; PIPELINES

;; Create a default Beam pipeline:
(th/create-pipeline)

;; Create a Beam pipeline from command-line arguments (Beam standard):
(th/create-pipeline ["--targetParallelism=7" "--jobName=thurber-walkthrough"])

;; Create a Beam pipeline from args provided as a Clojure map (skeleton-cased
;; keywords will map to Beam's camelCased option names):
(th/create-pipeline {:target-parallelism 7
                     :streaming true
                     :custom-config {:my-custom-config-val 5}})

;; (The :custom-config key is well-known to thurber and used to provide
;; dynamic options to pipelines. More on this later.)


;;;; SOURCES

;; We can create a data source from hard-coded Clojure values.
;; This is often useful for testing:
(def data-source (th/create [1 2 3]))

;; And of course any Java-based Beam source is valid, as well:
(def file-source (-> (TextIO/read) (.from "demo/word_count/lorem.txt")))


;;;; SINKS

;; Any Java-based Beam sink is thurber-ready:
(def file-sink (-> (TextIO/write) (.to "word-counts")))

;; Or we can sink to our logging system, which is very useful for testing:
(def log-sink #'th/log)


;;;; COMPOSING PIPELINES

;; thurber's `apply!` is used to build pipelines.

;; Here is the simplest of thurber pipelines -- it reads from a
;; source and writes to our log sink:
(def simplest-pipeline
  (doto (th/create-pipeline)
    (th/apply! data-source log-sink)))

;; Try it! This will log each input element from the
;; source (1, 2, 3...not necessarily in this order)
(.run simplest-pipeline)


;;;; ParDo TRANSFORMS

;; Here is a simple function:
(defn double [elem] (+ elem elem))

;; thurber treats Clojure functions as ParDo functions automatically:
(def simple-pipeline
  (doto (th/create-pipeline)
    (th/apply! data-source #'double log-sink)))

;; This logs 2 and 4 and 6 in some order:
(.run simple-pipeline)

;; Even keywords are coerced to ParDo transforms:
(def simple-pipeline-
  (doto (th/create-pipeline)
    (th/apply!
      (th/create [{:name :amy :age 110}
                  {:name :bob :age 100}])
      :name
      log-sink)))

;; This logs :bob and :amy...
(.run simple-pipeline-)

;;;; SERIALIZABLE FUNCTIONS

;; When constructing our pipeline above, why did we refer to
;; the function's var?
;;
;;      #'double
;;
;; Beam distributes functions across the cluster and vars
;; are serializable.
;;
;; In this case #'double is serialized  and sent to Beam cluster nodes.
;; When the var is deserialized, thurber ensures that it is rebound to
;; its function.


;;;; INLINE FUNCTIONS

;; The pipeline above refers to a named function in our
;; namespace. However thurber supports inlining functions,
;; which is useful in some cases for readability.

;; `thurber/fn*` creates an inline function. Inlined functions
;; must be given an explicit name:

(def simple-pipeline
  (doto (th/create-pipeline)
    (th/apply! data-source
      (th/fn* triple [elem]
        (* elem 3))
      log-sink)))

;; This logs 3, 6, and 9:
(.run simple-pipeline)


;;;; PARTIAL FUNCTIONS

;; During runtime stream processing, ParDo functions receive a
;; single element, the element being processed. However thurber
;; supports multi-arity ParDo functions where the last arg is
;; the processing element and prior (serializable) args are
;; bound early using `thurber/partial`:

(def simple-pipeline
  (doto (th/create-pipeline)
    (th/apply! data-source
      (th/partial #'* 4)
      log-sink)))

;; This logs 4, 8, and 12:
(.run simple-pipeline)


;;;; MULTIPLE OUTPUTS

;; A ParDo function can emit zero, one, or many values downstream.
;; When a seq (per Clojure's `seq?`) is returned from a function, all
;; values are emitted individually downstream.
;;
;; Lazy sequences are common when emitting large streams:
(defn to-words [^String sentence]
  (remove empty? (.split sentence "[^\\p{L}]+")))

(def words-pipeline
  (doto (th/create-pipeline)
    (th/apply!
      (th/create ["The quick brown fox jumps over the lazy dog."
                  "Pack my box with five dozen liquor jugs."])
      #'to-words
      log-sink)))

;; This logs each word in each sentence individually:
(.run words-pipeline)


;;;; GroupByKey TRANSFORMS

;; Most Beam pipelines require "shuffle" steps where related data is
;; gathered together by some value (i.e, by a "key" value). Before
;; a shuffle (e.g., `GroupByKey`) can occur, Beam requires the elements
;; to exist in KV form (as `org.apache.beam.sdk.values.KV` instances):

;; Let's group values in our source stream by whether they are even or odd:
(def example-pipeline
  (doto (th/create-pipeline)
    (th/apply! data-source
      ;; KV elements can be constructed by any ParDo function;
      ;; however thurber's th/->kv, with th/partial, is quite useful:
      (th/partial #'th/->kv
        (th/fn* classify-even-or-odd [v]
          (if (even? v) :even :odd)))
      (GroupByKey/create)
      log-sink)))

;; This logs grouped values, KV{:even [2]} and  KV{:odd [3, 1]}...
(.run example-pipeline)


;;;; COMPOSITE TRANSFORMS

;; Composite transforms can be created with `compose`:
(def count-even-and-odd-xf
  (th/compose "count-even-and-odd"
    (th/partial #'th/->kv #'classify-even-or-odd)
    (Count/perKey)
    ;; We can (optionally!) use kv->clj to convert Beam's Java KV values
    ;; to Clojure (i.e., MapEntry) values. This allows for downstream
    ;; destructuring of key/val pairs.
    #'th/kv->clj))

;; Using our composite transform:
(def example-pipeline
  (doto (th/create-pipeline)
    (th/apply!
      data-source
      count-even-and-odd-xf
      (th/fn* count-sink [[k v]]
        (log/infof "There are %d %s numbers."
          v (name k))))))

;; This logs "There are 2 odd numbers."
;;           "There are 1 even numbers."
(.run example-pipeline)


;;;; COMBINE TRANSFORMS

;; Beam's Combine transforms are like Clojure Reducers (https://clojure.org/reference/reducers),
;; and thurber uses similar concepts of reducef and combinef as `clojure.core.reducers/fold`.

(def example-pipeline
  (doto (th/create-pipeline)
    (th/apply!
      data-source
      (Combine/globally
        (th/combiner #'+))
      #'th/log)))

(.run example-pipeline) ;; => logs "6"

;;;; NAMING TRANSFORMS

;; thurber and Beam infer sensible transform names when no explicit name
;; is given, but it is good practice to be explicit when there is potential
;; for conflicts.

;; `partial`, `filter`, and `create` all support an optional explicit transform
;; name as a first arg, and `apply!` can be given an optional string that will
;; be used to prefix every step name.

;; Here is a pipeline that forks with two branches. Even though explicit step
;; names are duplicated, the prefix supplied to `apply!` will ensure the step
;; names are unique:
(def example-pipeline
  (doto (th/create-pipeline)
    (th/apply! "odd-handling"
      (th/create "source-data" [1 2 3])
      (th/filter "filter-odds" #'odd?)
      (th/partial "multiply-by-5" #'* 5)
      #'th/log)
    (th/apply! "even-handling"
      (th/create "source-data" [1 2 3])
      (th/filter "filter-evens" #'even?)
      (th/partial "subtract-one" #'dec)
      #'th/log)))

;; A composite transform (`th/compose`) can be given an optional name:
(def my-xf (th/compose "inc-and-negate" #'inc #'-))

;;;; CODERS
;;
;; By default thurber codes Clojure (and Java-Serializable) elements using the same
;; de/serializer, high-performance nippy (https://github.com/ptaoussanis/nippy), but
;; Beam supports an array  of specialized coders, as well as user-defined custom coders,
;; and thurber is fully compatible here.

;; thurber allows coders to be explicitly specified. Here we code our `data-source`
;; using Beam's `VarLongCoder` in place of thurber's default nippy coder:
(def example-pipeline
  (doto (th/create-pipeline)
    (th/apply!
      {:th/xform data-source
       :th/coder (VarLongCoder/of)}
      #'th/log)))

;; Clojure transform functions can explicitly specify coders in their metadata:
(defn ^{:th/coder th/nippy-kv} convert-to-kv [element]
  (KV/of (:color element) element))

;;;; CONTEXT BINDINGS

;; thurber ParDo functions return value(s) to be emitted downstream but
;; some complicated ParDo functions need access to element timestamps,
;; windows and pane information, etc.
;;
;; thurber exposes all of beams per-element context information via thread-local
;; bindings (for performance, thurber does not use clojure dynamic bindings).
;;
;; thurber uses a *single-earmuff to call out these dynamic-bound context
;; values (as opposed to the *earmuff* convention for dynamic bindings).
;;
;; Here is a ParDo that only emits the current element only if the firing pane
;; is on time; it obtains pane timing information from the current `ProcessContext`:
(defn- filter-on-time-panes [elem]
  (let [pane-timing (-> (th/*process-context) (.pane) (.getTiming))]
    (when (= pane-timing PaneInfo$Timing/ON_TIME)
      elem)))

;; thurber supports dynamic "custom config" input to a pipeline.
;; Beam's PipelineOptions are available to ParDo functions via
;; `thurber/*pipeline-options` and custom config via `thurber/*custom-config`:
(def example-pipeline
  (doto (th/create-pipeline {:custom-config {:count-me 2}})
    (th/apply!
      data-source
      (th/filter
        (th/fn* just-count-me [elem]
          (= elem (-> (th/*custom-config) :count-me))))
      (Combine/globally (th/combiner #'+))
      #'filter-on-time-panes
      #'th/log)))

;;;; SIDE INPUTS

;; Side inputs are used as additional inputs to ParDo transforms.
;; As `PCollectionView`s are serializable we can pass them as `partial`
;; args to any ParDo Clojure function:
(defn- above-mean? [^PCollectionView mean-view elem]
  (let [mean (.sideInput (th/*process-context) mean-view)]
    (> elem mean)))

;; This pipeline use a side view to counts all values in our data stream
;; that are above the overall mean:
(def example-pipeline
  (let [pipeline (th/create-pipeline)
        data (th/apply! pipeline
               (th/create (range 1 100)))
        mean-view (th/apply! data "mean-view"
                    (Mean/globally)
                    (View/asSingleton))]
    (th/apply!
      data
      (th/filter #'above-mean? mean-view)
      (Count/globally)
      #'th/log)
    pipeline))

;; This logs "49":
(.run example-pipeline)

;;;; STATE AND TIMERS

;; transforms annotated with `:th/stateful` can access a Beam value state and bag
;; state via `(th/*value-state)` and `(th/*bag-state)`

;; An implementation of group-into-batches leverages both value and bag states:
(defn- ^{:th/stateful true :th/coder th/nippy} group-into-batches [batch-size ^KV elem]
  (let [counter (-> (.read (th/*value-state)) (or 0) inc)
        _ (.write (th/*value-state) counter)]
    (.add (th/*bag-state) (.getValue elem))
    (when (<= batch-size counter)
      (.output (th/*process-context) (vec (.read (th/*bag-state))))
      (.write (th/*value-state) 0)
      (.clear (th/*bag-state)))))

(def example-pipeline
  (doto (th/create-pipeline)
    (th/apply!
      (th/create (range 15))
      (WithKeys/of "global")
      (th/partial #'group-into-batches 5)
      #'th/log)))

;; This logs three batches of five elements each:
(.run example-pipeline)

;; Of course the above implementation won't emit any incomplete last batch. To fix
;; this we would need to use a Beam Timer.

;; For a simpler example, suppose we wanted to use a Beam Timer to ensure our event
;; stream has an element every millisecond; for ever millisecond that we do not have
;; an element in our event stream, we will emit a marker "<missing>" value:
(defn- emit-missing-timer [^Instant stop-time]
  (when (.isBefore (.timestamp (th/*timer-context)) stop-time)
    (-> (th/*event-timer)
      (.offset (Duration/millis 1))
      (.setRelative)))
  "<missing>")

;; When we have an element, we will reset the timer to the next millisecond:
(defn- emit-missing [^KV elem]
  (-> (th/*event-timer)
    (.offset (Duration/millis 1))
    (.setRelative))
  (.getValue elem))

(def example-pipeline
  (doto (th/create-pipeline)
    (th/apply!
      (th/create (remove odd? (range 10)))
      (WithTimestamps/of (th/ser-fn
                           (th/fn* to-instant [n]
                             (Instant. n))))
      (WithKeys/of "global")
      {:th/xform #'emit-missing
       :th/timer-fn #'emit-missing-timer
       :th/timer-params [(Instant. 10)]
       :th/coder th/nippy}
      #'th/log)))

;; This logs 0, "<missing>", 2, "<missing>", 4, "<missing>", 6,
;;   "<missing>", 8, "<missing>"  in some order:
(.run example-pipeline)

;;;; JAVA INTEROP

;; thurber directly supports any Java-based transform, so transforms from Beam's SDK
;; or other Java libraries are thurber-ready. In some cases these transforms are configured
;; with `SerializableFunction`, `SerializableBiFunction`; thurber allows a Clojure function
;; to be converted to one of these types to facilitate interop w/ Java-based transforms.
;;
;; `ser-fn` converts a Clojure function to a `SerializableFunction` or `SerializableBiFunction`
;; depending on the arity of the provided function:

(defn- multiply-element [n]
  (repeat n n))

(def example-pipeline
  (doto (th/create-pipeline)
    (th/apply! data-source
      (-> (FlatMapElements/into
            (TypeDescriptors/longs))
        (.via
          (th/ser-fn #'multiply-element)))
      #'th/log)))

(.run example-pipeline) ;; logs 1, 2, 2, 3, 3, 3 in some order

;;;; FACADE

;; The `thurber` namespace attempts to provide a minimal yet fully capable and expressive
;; set of facilities to build Beam pipelines using first-class Clojure constructs.

;; Some users will find Java interop with Beam's fluent APIs a bit prickly and will want
;; to write Clojurey versions of transform builders, etc; we leave such "facade" functions
;; to user; these are easy to write and tend to be domain-specific.

;; As an example of what this might look like, consider word count here written with
;; Clojurey transform builders:

(require '[thurber.sugar :refer [read-text-file count-per-element]])

(def example-pipeline
  (doto (th/create-pipeline)
    (th/apply!
      (read-text-file
        "demo/word_count/lorem.txt")
      (th/fn* extract-words [^String sentence]
        (remove empty? (.split sentence "[^\\p{L}]+")))
      (count-per-element)
      (th/fn* format-as-text
        [[k v]] (format "%s: %d" k v))
      log-sink)))

;; This logs all word counts from lorem.txt:
(.run example-pipeline)