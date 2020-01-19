(ns thurber.java.options
  (:require [clojure.test :refer :all]
            [thurber :as th])
  (:import (org.apache.beam.runners.direct DirectOptions)))

(deftest test-options
  (is (= 7
        (-> (th/create-options
              {:target-parallelism 7
               :custom-config {:message :contact}})
          ^DirectOptions (.as DirectOptions)
          (.getTargetParallelism))))
  (is (= 7
        (-> (th/create-options
              ["--targetParallelism=7" "--customConfig={\"message\": \"contact\"}"])
          ^DirectOptions (.as DirectOptions)
          (.getTargetParallelism)))))

(deftest test-custom-config
  (is (= {} (-> (th/create-options) th/get-custom-config)))
  (is (= {} (-> (th/create-options {}) th/get-custom-config)))
  (is (= {} (-> (th/create-options {:target-parallelism 7
                                    :custom-config {}}) th/get-custom-config)))
  (is (= {:message "contact"}
        (-> (th/create-options
              {:target-parallelism 7
               :custom-config {:message :contact}})
          th/get-custom-config)))
  (is (= {:message "contact"}
        (-> (th/create-options
              ["--targetParallelism=7" "--customConfig={\"message\": \"contact\"}"])
          th/get-custom-config))))