(ns demo.word-count-test
  (:require [clojure.test :refer :all]
            [thurber :as th]
            [word-count.basic]
            [word-count.windowed]
            [clj-time.coerce :as c]
            [clj-time.core :as t]
            [clojure.string :as str])
  (:import (org.apache.beam.sdk.testing PAssert)))

;;; Here we test the word-count pipeline by composing some if
;;; its known parts (composite transforms and functions).
;;;
;;; For higher-level testing of an entire pipeline see the next
;;; deftest.

(deftest test-word-count-basic
  (let [output (th/apply! (test-support/create-test-pipeline)
                 (th/create
                   ["hi there" "hi" "hi sue bob" "hi sue" "" "bob hi"])
                 word-count.basic/count-words-xf
                 #'word-count.basic/format-as-text)]
    (-> output
      (PAssert/that)
      (.containsInAnyOrder
        ["hi: 5" "there: 1" "sue: 2" "bob: 2"]))
    (test-support/run-test-pipeline! output)))

;;; Using Clojure's with-redefs, we can fake a pipeline's
;;; source(s) and mock its sink(s) to achieve a more end-to-end
;;; test over the pipeline as a whole.

(deftest test-word-count-windowed
  (let [now (t/now)
        mock-sink (atom {})
        mock-sink-reducer (fn [acc s]
                            (let [[word n] (str/split s #": ")]
                              (update acc word (fnil + 0) (Integer/parseInt n))))]
    (with-redefs [word-count.windowed/->text-io-xf
                  (fn [_]
                    (th/create ["hi there" "hi" "hi sue bob" "hi sue" "" "bob hi"]))
                  word-count.windowed/sink* (partial swap! mock-sink mock-sink-reducer)]
      (-> (test-support/create-test-pipeline
            {:custom-config
             {:window-size 30
              :min-timestamp (c/to-long now)
              :max-timestamp (c/to-long (t/plus now (t/hours 1)))}})
        (#'word-count.windowed/build-pipeline!)
        test-support/run-test-pipeline!)
      (is (= {"bob" 2 "hi" 5 "sue" 2 "there" 1}
            @mock-sink)))))
