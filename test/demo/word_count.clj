(ns demo.word-count
  (:require [clojure.test :refer :all]
            [thurber :as th]
            [word-count.basic])
  (:import (org.apache.beam.sdk.testing TestPipeline PAssert)))

(deftest test-count-words
  (let [p (-> (TestPipeline/create)
            (.enableAbandonedNodeEnforcement true))
        output (th/apply! p
                 (th/create
                   ["hi there" "hi" "hi sue bob" "hi sue" "" "bob hi"])
                 word-count.basic/count-words-xf
                 #'word-count.basic/format-as-text)]
    (-> output
      (PAssert/that)
      (.containsInAnyOrder
        ["hi: 5" "there: 1" "sue: 2" "bob: 2"]))
    (-> (.run p)
      (.waitUntilFinish))))
