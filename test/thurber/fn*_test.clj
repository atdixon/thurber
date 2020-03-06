(ns thurber.fn*-test
  (:require [clojure.test :refer :all]
            [thurber :as th])
  (:import (org.apache.beam.sdk.coders StringUtf8Coder)))

(deftest test-fn*
  (is (= :alpha
        ((th/fn* alpha []
           :alpha))))
  (is (= :beta
        (let [x :beta]
          (.invoke_
            (th/fn* beta []
              x)))))
  ;; here we can see x is still masked by
  ;;   the fn* param:
  (is (= :gamma
        (let [x :delta]
          (.invoke_
            (th/fn* gamma [x]
              x) :gamma))))
  ;; metadata on th/fn*
  (is (= {:name 'delta
          :ns (find-ns 'thurber.fn*-test)
          :th/coder (StringUtf8Coder/of)}
        (meta
          (th/fn* ^{:th/coder (StringUtf8Coder/of)} delta [x]
            x)))))
