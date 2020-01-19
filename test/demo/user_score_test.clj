(ns demo.user-score-test
  (:require [clojure.test :refer :all]
            [game.user-score]
            [game.user-score-opt]
            [thurber :as th]
            [test-support])
  (:import (org.apache.beam.sdk.testing PAssert)))

;; Port of Beam's org.apache.beam.examples.complete.game.UserScoreTest.java

(def ^:private game-events
  ["user0_MagentaKangaroo,MagentaKangaroo,3,1447955630000,2015-11-19 09:53:53.444"
   "user13_ApricotQuokka,ApricotQuokka,15,1447955630000,2015-11-19 09:53:53.444"
   "user6_AmberNumbat,AmberNumbat,11,1447955630000,2015-11-19 09:53:53.444"
   "user7_AlmondWallaby,AlmondWallaby,15,1447955630000,2015-11-19 09:53:53.444"
   "user7_AndroidGreenKookaburra,AndroidGreenKookaburra,12,1447955630000,2015-11-19 09:53:53.444"
   "user6_AliceBlueDingo,AliceBlueDingo,4,xxxxxxx,2015-11-19 09:53:53.444"
   "user7_AndroidGreenKookaburra,AndroidGreenKookaburra,11,1447955630000,2015-11-19 09:53:53.444"
   "THIS IS A PARSE ERROR,2015-11-19 09:53:53.444"
   "user19_BisqueBilby,BisqueBilby,6,1447955630000,2015-11-19 09:53:53.444"
   "user19_BisqueBilby,BisqueBilby,8,1447955630000,2015-11-19 09:53:53.444"])

(def ^:private game-events-2
  ["user6_AliceBlueDingo,AliceBlueDingo,4,xxxxxxx,2015-11-19 09:53:53.444"
   "THIS IS A PARSE ERROR,2015-11-19 09:53:53.444"
   "user13_BisqueBilby,BisqueBilby,xxx,1447955630000,2015-11-19 09:53:53.444"])

(def ^:private game-action-infos
  [{:user "user0_MagentaKangaroo" :team "MagentaKangaroo" :score (int 3) :timestamp 1447955630000}
   {:user "user13_ApricotQuokka" :team "ApricotQuokka" :score (int 15) :timestamp 1447955630000}
   {:user "user6_AmberNumbat" :team "AmberNumbat" :score (int 11) :timestamp 1447955630000}
   {:user "user7_AlmondWallaby" :team "AlmondWallaby" :score (int 15) :timestamp 1447955630000}
   {:user "user7_AndroidGreenKookaburra" :team "AndroidGreenKookaburra" :score (int 12) :timestamp 1447955630000}
   {:user "user7_AndroidGreenKookaburra" :team "AndroidGreenKookaburra" :score (int 11) :timestamp 1447955630000}
   {:user "user19_BisqueBilby" :team "BisqueBilby" :score (int 6) :timestamp 1447955630000}
   {:user "user19_BisqueBilby" :team "BisqueBilby" :score (int 8) :timestamp 1447955630000}])

(def ^:private user-sums
  [["user0_MagentaKangaroo" (int 3)]
   ["user13_ApricotQuokka" (int 15)]
   ["user6_AmberNumbat" (int 11)]
   ["user7_AlmondWallaby" (int 15)]
   ["user7_AndroidGreenKookaburra" (int 23)]
   ["user19_BisqueBilby" (int 14)]])

(def ^:private team-sums
  [["MagentaKangaroo" (int 3)]
   ["ApricotQuokka" (int 15)]
   ["AmberNumbat" (int 11)]
   ["AlmondWallaby" (int 15)]
   ["AndroidGreenKookaburra" (int 23)]
   ["BisqueBilby" (int 14)]])

(deftest test-parse-event-fn
  (let [pipeline (test-support/create-test-pipeline)]
    (-> pipeline
      (th/apply!
        (th/create game-events)
        #'game.user-score/parse-event)
      (-> (PAssert/that)
        (.containsInAnyOrder ^Iterable game-action-infos)))
    (test-support/run-test-pipeline! pipeline)))

(deftest test-user-score-sums
  (let [pipeline (test-support/create-test-pipeline)]
    (-> pipeline
      (th/apply!
        (th/create game-events)
        #'game.user-score/parse-event
        (game.user-score/->extract-sum-and-score-xf :user))
      (-> (PAssert/that)
        (.containsInAnyOrder ^Iterable user-sums)))
    (test-support/run-test-pipeline! pipeline)))

(deftest test-team-score-sums
  (let [pipeline (test-support/create-test-pipeline)]
    (-> pipeline
      (th/apply!
        (th/create game-events)
        #'game.user-score/parse-event
        (game.user-score/->extract-sum-and-score-xf :team))
      (-> (PAssert/that)
        (.containsInAnyOrder ^Iterable team-sums)))
    (test-support/run-test-pipeline! pipeline)))

(deftest test-user-scores-bad-input
  (let [pipeline (test-support/create-test-pipeline)]
    (-> pipeline
      (th/apply!
        (th/create game-events-2)
        #'game.user-score/parse-event)
      (-> (PAssert/that)
        (.empty)))
    (test-support/run-test-pipeline! pipeline)))

(deftest test-team-score-opt-sums
  (let [pipeline (test-support/create-test-pipeline)]
    (-> pipeline
      (th/apply!
        (th/create game-events)
        #'game.user-score-opt/parse-event
        (game.user-score-opt/->extract-sum-and-score-xf :team))
      (-> (PAssert/that)
        (.containsInAnyOrder ^Iterable (map th/clj->kv team-sums))))
    (test-support/run-test-pipeline! pipeline)))