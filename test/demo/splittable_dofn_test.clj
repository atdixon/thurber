(ns demo.splittable-dofn-test
  (:require [clojure.test :refer :all]
            [thurber :as th]
            [test-support]
            [simple.splittable-dofn :as sut]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.transforms.windowing BoundedWindow PaneInfo$Timing)
           (org.apache.beam.sdk PipelineResult)))

(def ^:private test-buffer (atom nil))

(defn- delay*
  ([] (delay* 2))
  ([n] (Thread/sleep (* n (.getMillis sut/resume-delay)))))

(deftest test-splittable-dofn
  (reset! test-buffer [])
  (with-redefs [th/log-verbose (comp
                                 (constantly nil)
                                 (partial swap! test-buffer conj)
                                 (fn [elem]
                                   {:elem elem
                                    :on-time? (= (-> (th/*process-context)
                                                   (.pane) (.getTiming))
                                                PaneInfo$Timing/ON_TIME)}))]
    (try
      (sut/demo!)

      (sut/update-timestamp!
        #inst "2019-12-31T00:00:00.000Z")
      (sut/put-element! :new-years-eve-1)
      (delay*)

      (sut/update-timestamp!
        #inst "2019-12-31T12:00:00.000Z")
      (sut/put-element! :new-years-eve-2)
      (delay*)

      (sut/update-watermark!
        #inst "2020-01-01T00:00:00.000Z")
      (delay*)

      (is (= @test-buffer [{:elem 1 :on-time? false}
                           {:elem 2 :on-time? false}
                           {:elem 2 :on-time? true}]))
      (sut/update-watermark!
        BoundedWindow/TIMESTAMP_MAX_VALUE)
      (.waitUntilFinish
        ^PipelineResult @sut/latest-pipeline-result)
      (finally
        (sut/cancel-demo!)))))
