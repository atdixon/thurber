(ns thurber.combine-test
  (:require [clojure.test :refer :all]
            [thurber :as th]
            [test-support])
  (:import (org.apache.beam.sdk.testing PAssert)
           (org.apache.beam.sdk.transforms Combine)))

(deftest test-combine
  (let [output (th/apply! (test-support/create-test-pipeline)
                 (th/create (range 10))
                 (Combine/globally
                   (th/combiner #'+)))]
    (-> output
      (PAssert/that) (.containsInAnyOrder [45]))
    (test-support/run-test-pipeline! output)))

(deftest test-combine-complex
  (let [output (th/apply! (test-support/create-test-pipeline)
                 (th/create (range 1 6))
                 ;; compute overall mean via combine:
                 (Combine/globally
                   (th/combiner
                     (th/fn* test-complex-extractf [acc]
                       (double (if (zero? (:count acc)) 0 (/ (:sum acc) (:count acc)))))
                     (th/fn* test-complex-combinef [& accs]
                       (apply merge-with + accs))
                     (th/fn* test-complex-reducef
                       ([] {:count 0 :sum 0})
                       ([acc inp] (-> acc
                                    (update :count inc)
                                    (update :sum + inp)))))))]
    (-> output
      (PAssert/that) (.containsInAnyOrder [3.0]))
    (test-support/run-test-pipeline! output)))