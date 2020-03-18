(ns thurber.regressions-test
  (:require [clojure.test :refer :all]
            [thurber :as th]
            [test-support]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.testing PAssert)
           (org.apache.beam.sdk.transforms View Min Count)))

(deftest test-filter-with-anon-fn
  (let [data-stream
        (th/apply!
          (test-support/create-test-pipeline)
          (th/create [1 2 1 6 1 3 1 8]))
        min-view (th/apply!
                   data-stream
                   (Min/globally)
                   (View/asSingleton))
        min-count (th/apply!
                    data-stream
                    (th/filter
                      (th/fn* is-min? [elem]
                        (= elem (th/*side-input min-view))))
                    (Count/globally))]
    (-> min-count
      (PAssert/that) (.containsInAnyOrder [4]))
    (test-support/run-test-pipeline! data-stream)))

(deftest test-nested-partial
  (let [data-stream
        (th/apply!
          (test-support/create-test-pipeline)
          (th/create [6 1 2 1 6 1 3 1 6 8]))
        min-count (th/apply!
                    data-stream
                    (th/filter
                      (th/partial
                        (th/partial
                          (th/fn* is-counted? [count-val-max-exclusive
                                               count-val-min-exclusive
                                               elem]
                            (< count-val-min-exclusive elem count-val-max-exclusive)) 1) 8))
                    (Count/globally))]
    (-> min-count
      (PAssert/that) (.containsInAnyOrder [5]))
    (test-support/run-test-pipeline! data-stream)))
