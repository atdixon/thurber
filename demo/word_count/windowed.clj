(ns word-count.windowed
  (:require [thurber :as th]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.tools.logging :as log]
            [word-count.basic])
  (:import (org.apache.beam.sdk.io TextIO)
           (org.joda.time Duration Instant)
           (org.apache.beam.sdk.transforms.windowing Window FixedWindows)))

;; clj-time is used because Beam standardizes on Joda time.

;; A function that creates our TextIO source transform. We could
;; inline this in our pipeline construction (see other demos) but
;; creating as a function allows us for reuse and for mocking (via
;; with-redefs) for testing (see unit tests).
(defn- ->text-io-xf
  [custom-conf]
  (-> (TextIO/read)
    (.from ^String (:input-file custom-conf))))

;; Thurber DoFn functions can take extra parameters at the front of their arg
;; lists. Additional args to `th/pardo*` provide these serializable parameters,
;; (often config values) to these functions.
(defn- add-timestamp [{:keys [min-timestamp max-timestamp]} sentence]
  (let [random-timestamp (->> (rand-int (- max-timestamp min-timestamp))
                              (+ min-timestamp)
                              (Instant.))]
    ;; Instead of returning the value to emit, our DoFn functions can emit
    ;; to Beam directly. This is useful in this case for emitting with an
    ;; explicit timestamp. Note that by returning nil/void from our DoFn function,
    ;; Thurber will not emit any elements on our behalf.
    (.outputWithTimestamp (th/*process-context) sentence random-timestamp)))

(defn- sink* [elem]
  ;; We can access an element's window from within a DoFn.
  (log/infof "%s in %s" elem (th/*element-window)))

(defn- build-pipeline! [pipeline]
  (let [conf (th/get-custom-config pipeline)]
    (th/apply! pipeline
      (->text-io-xf conf)
      (th/partial #'add-timestamp conf)
      ;; Here we window into fixed windows. There is no need for Thurber to
      ;; to try to sugar-coat Beam window configuration; Clojure's Java interop
      ;; works perfectly fine in this case.
      (Window/into
        (FixedWindows/of
          (Duration/standardMinutes (:window-size conf))))
      word-count.basic/count-words-xf
      #'word-count.basic/format-as-text
      #'sink*)
    pipeline))

(defn demo! []
  (let [now (t/now)]
    (->
      (th/create-pipeline
        {:custom-config {:input-file "demo/word_count/lorem.txt"
                         :window-size 30
                         :min-timestamp (c/to-long (t/now))
                         :max-timestamp (c/to-long (t/plus now (t/hours 1)))}})
      (build-pipeline!)
      (.run))))
