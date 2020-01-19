(ns walkthrough
  (:import (org.apache.beam.sdk.transforms GroupByKey Combine)))

;; This walkthrough introduces the core concepts of thurber.

;; The thurber namespace contains the public API.
(require '[thurber :as th])

;; Beam standardizes on Joda time and slf4j logging:
(require '[clj-time.core :as t]
         '[clojure.tools.logging :as log])

;; It will be common to interop with Beam's Java classes:
(import 'org.apache.beam.runners.direct.DirectOptions
        'org.apache.beam.sdk.io.TextIO
        'org.apache.beam.sdk.values.KV
        'org.apache.beam.sdk.transforms.Count)

;;;; PIPELINES

;; Create a default Beam pipeline:
(th/create-pipeline)

;; Create a Beam pipeline from command-line arguments:
(def pipeline-from-args
  (th/create-pipeline ["--targetParallelism=7" "--jobName=thurber-walkthrough"]))

(assert (= "thurber-walkthrough" (-> pipeline-from-args (.getOptions) (.getJobName))))

;; Create a Beam pipeline from args provided as a Clojure map:
(def pipeline-from-args-map
  (th/create-pipeline {:target-parallelism 7
                       :job-name "thurber-walkthrough"}))

(assert (= "thurber-walkthrough" (-> pipeline-from-args-map (.getOptions) (.getJobName))))
(assert (= 7 (-> pipeline-from-args-map (.getOptions) (.as DirectOptions) (.getTargetParallelism))))

;; A "custom config" arg can be provided; this allows for dynamic arguments (i.e., no need
;; to define a static class extension to PipelineOptions). The custom config will be available
;; to pipeline (at pipeline construction-time or run-time) as a Clojure map.
(def pipeline-with-custom-config
  (th/create-pipeline {:target-parallelism 11
                       :custom-config {:my-custom-config-val 5}}))

(assert (= 5 (:my-custom-config-val (th/get-custom-config pipeline-with-custom-config))))

;;;; SOURCES

;; Pipelines read from sources.

;; We can create a source from hard-coded Clojure data. This is often
;; useful for testing:
(def data-source (th/create [1 2 3]))

;; We can also create any Beam Java-based source:
(def file-source (-> (TextIO/read) (.from "word_count/lorem.txt")))

;;;; SINKS

;; Pipelines write to sinks.

;; We can create a sink using any of Beam Java-based sink:
(def file-sink (-> (TextIO/write) (.to "word-counts")))

;; We can simply sink to our logging system. This is often useful
;; for testing:
(def log-sink #'th/log-elem*)

;;;; SIMPLEST PIPELINE

;; thurber's `apply!` is used to build pipelines.

;; Here we read from our simple hard-coded source and write to our
;; log sink:
(def simplest-pipeline
  (doto (th/create-pipeline)
    (th/apply! data-source log-sink)))

;; Run the pipeline. This will log each input element from the
;; source (1, 2, 3...not necessarily in this order)
(.run simplest-pipeline)

;;;; FUNCTIONS (ParDo)

;; The simplest Beam transform is a ParDo ("parallel do").

;; Here is a simple function:
(defn double [elem] (+ elem elem))

;; thurber treats Clojure functions as a ParDos automatically:
(def simple-pipeline
  (doto (th/create-pipeline)
    (th/apply! data-source #'double log-sink)))

;; This logs 2 and 4 and 6 in some order:
(.run simple-pipeline)

;;;; SERIALIZABLE FUNCTIONS

;; When constructing our pipeline, why did we refer to
;; the function's var?
;;
;;      #'double
;;
;; Beam distributes functions across the cluster and vars
;; are serializable. In this case #'double is serialized
;; and sent to Beam cluster nodes. When the var is deserialized,
;; thurber ensures that it is rebound to its function.

;;;; INLINE FUNCTIONS

;; The pipeline above refers to a named function in our
;; namespace. However thurber supports inlining functions,
;; which is useful in some cases for readability.

;; thurber's `inline` must be used, which ensures the inline
;; function is properly serializable. Inlined functions
;; must be given an explicit name:

(def simple-pipeline
  (doto (th/create-pipeline)
    (th/apply! data-source
      (th/inline
        (fn triple [elem]
          (* elem 3)))
      log-sink)))

;; This logs 3, 6, and 9:
(.run simple-pipeline)

;;;; PARTIAL FUNCTIONS

;; During runtime stream processing, ParDo functions receive a
;; single element, the element being processed. However thurber
;; supports multi-arity ParDo functions where the last arg is
;; the processing element and prior (serializable) args are
;; bound early using th/partial*:

(def simple-pipeline
  (doto (th/create-pipeline)
    (th/apply! data-source
      (th/partial* #'* 4)
      log-sink)))

;; This logs 4, 8, and 12:
(.run simple-pipeline)

;;;; MULTIPLE OUTPUTS

;; A ParDo function can emit zero, one, or many values downstream.
;; When a seq (per Clojure's seq?) is returned from a function, all
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

;;;; GROUP BY KEY

;; Most Beam pipelines require "shuffle" steps where related data is
;; gathered together by some value (i.e, by a "key" value). Before
;; a shuffle (e.g., GroupByKey) can occur, Beam requires the elements
;; to exist in KV form (as org.apache.beam.sdk.values.KV instances):

;; Let's group values in our source stream by whether they are even or odd:
(def example-pipeline
  (doto (th/create-pipeline)
    (th/apply! data-source
      ;; KV elements can be constructed by any ParDo function;
      ;; however thurber's th/->kv, with th/partial*, is quite useful:
      (th/partial* #'th/->kv
        (th/inline
          (fn classify-even-or-odd [v]
            (if (even? v) :even :odd))))
      (GroupByKey/create)
      log-sink)))

;; This logs grouped values, KV{:even [2]} and  KV{:odd [3, 1]}...
(.run example-pipeline)

;;;; COMPOSITE TRANSFORMS

;; Composite transforms can be created with `comp*`:
(def count-even-and-odd-xf
  (th/comp* "count-even-and-odd"
    (th/partial* #'th/->kv #'classify-even-or-odd)
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
      (th/inline
        (fn count-sink [[k v]]
          (log/infof "There are %d %s numbers."
            v (name k)))))))

;; This logs "There are 2 odd numbers."
;;           "There are 1 even numbers."
(.run example-pipeline)

;;;; COMBINE

;; Beam's Combine transforms are like Clojure Reducers (https://clojure.org/reference/reducers),
;; and thurber uses similar concepts of reducef and combinef as `clojure.core.reducers/fold`.

(def example-pipeline
  (doto (th/create-pipeline)
    (th/apply!
      data-source
      (Combine/globally
        (th/combiner #'+))
      #'th/log-elem*)))

;; The combine sum, 6, is logged:
(.run example-pipeline)

;;;;

;; todo optional name,, name prefixes
;; todo coders!!!
;; todo ser-fn
;; todo thread-local bindings etc.
;; todo state and timer API
;; todo output tags
;; todo side inputs