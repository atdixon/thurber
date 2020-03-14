(ns simple.hello-world
  (:require [thurber :as th]))

(defn- build-pipeline! [pipeline]
  (doto pipeline
    (th/apply!
     (th/create ["hello world"])
     ;; th/fn* allows named functions to be "inlined" in a pipeline
     ;; expression; this adds in expression and readability.
     ;;
     ;; th/fn* will ensure the named function is defined for the pipeline
     ;; namespace so that fn serialization and deserialization/rebinding
     ;; properly occurs.
     ;;
     ;; Beam functions however can not access local lexical closure and
     ;; should obtain "configuration" either by consult the pipeline's
     ;; PipelineOptions (via th/*pipeline-options) or by using th/partial
     ;; to explicitly pass serializable args/config to a function.
     ;;
     ;; Here we access our custom config map via th/*custom-config (carried
     ;; by PipelineOptions):
      (th/fn* prefix* [elem]
        (str (:prefix (th/*custom-config)) elem))
     #'th/log)))

(defn demo! []
  (->
    (th/create-pipeline
        {:custom-config {:prefix "message: "}})
    build-pipeline! .run))
