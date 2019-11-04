(ns thurber.apply-dofn
  (:import (org.apache.beam.sdk.transforms DoFn$ProcessContext)
           (clojure.lang MapEntry)
           (org.apache.beam.sdk.values KV)
           (org.apache.beam.sdk.transforms.windowing BoundedWindow)
           (org.apache.beam.sdk.options PipelineOptions)))

(def ^:dynamic ^PipelineOptions *pipeline-options* nil)
(def ^:dynamic ^DoFn$ProcessContext *process-context* nil)
(def ^:dynamic ^BoundedWindow *element-window* nil)

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
