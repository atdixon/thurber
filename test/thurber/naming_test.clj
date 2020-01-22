(ns thurber.naming-test
  (:require [clojure.test :refer :all]
            [thurber :as th]
            [test-support]
            [clojure.string :as str])
  (:import (org.apache.beam.sdk.testing PAssert)))

(deftest test-explicit-naming
  (let [output (th/apply! "prefix"
                 (test-support/create-test-pipeline)
                 (th/create "create" (range 10))
                 (th/compose "add-2" #'inc {:th/name "inc-again"
                                            :th/xform #'inc})
                 (th/filter "filter-odds" #'odd?)
                 (th/partial "add-2-again" #'+ 2))]
    (-> output
      (PAssert/that) (.containsInAnyOrder [5 7 9 11 13]))
    (test-support/run-test-pipeline! output)))

(deftest test-explicit-naming-conflicts
  (try
    (test-support/run-test-pipeline!
      (th/apply! "prefix"
        (test-support/create-test-pipeline)
        (th/create "create" (range 10))
        (th/compose "add-2" #'inc)
        (th/partial "add-2" #'+ 2)))
    (is false "expected exception")
    (catch IllegalStateException e
      (is (str/includes? (.getMessage e)
            "the following transforms do not have stable unique names"))))
  (try
    (test-support/run-test-pipeline!
      (th/apply!
        (test-support/create-test-pipeline)
        (th/create "create" (range 10))
        (th/compose "add-2" #'inc #'inc)))
    (is false "expected exception")
    (catch IllegalStateException e
      (is (str/includes? (.getMessage e)
            "the following transforms do not have stable unique names")))))

