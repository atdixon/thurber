(ns demo.hourly-team-score-test
  (:require [clojure.test :refer :all]
            [game.user-score]
            [game.hourly-team-score]
            [test-support]
            [thurber :as th])
  (:import (org.joda.time Instant)
           (org.apache.beam.sdk.testing PAssert)))

(def ^:private game-events
  [["user0_MagentaKangaroo,MagentaKangaroo,3,1447955630000,2015-11-19 09:53:53.444"]
   ["user13_ApricotQuokka,ApricotQuokka,15,1447955630000,2015-11-19 09:53:53.444"]
   ["user6_AmberNumbat,AmberNumbat,11,1447955630000,2015-11-19 09:53:53.444"]
   ["user7_AlmondWallaby,AlmondWallaby,15,1447955630000,2015-11-19 09:53:53.444"]
   ["user7_AndroidGreenKookaburra,AndroidGreenKookaburra,12,1447955630000,2015-11-19 09:53:53.444"]
   ["user7_AndroidGreenKookaburra,AndroidGreenKookaburra,11,1447955630000,2015-11-19 09:53:53.444"]
   ["user19_BisqueBilby,BisqueBilby,6,1447955630000,2015-11-19 09:53:53.444"]
   ["user19_BisqueBilby,BisqueBilby,8,1447955630000,2015-11-19 09:53:53.444"]
   ; time gap...
   ["user0_AndroidGreenEchidna,AndroidGreenEchidna,0,1447965690000,2015-11-19 12:41:31.053"]
   ["user0_MagentaKangaroo,MagentaKangaroo,4,1447965690000,2015-11-19 12:41:31.053"]
   ["user2_AmberCockatoo,AmberCockatoo,13,1447965690000,2015-11-19 12:41:31.053"]
   ["user18_BananaEmu,BananaEmu,7,1447965690000,2015-11-19 12:41:31.053"]
   ["user3_BananaEmu,BananaEmu,17,1447965690000,2015-11-19 12:41:31.053"]
   ["user18_BananaEmu,BananaEmu,1,1447965690000,2015-11-19 12:41:31.053"]
   ["user18_ApricotCaneToad,ApricotCaneToad,14,1447965690000,2015-11-19 12:41:31.053"]])

(deftest test-user-scores-filter
  (let [start-min-ts (Instant. 1447965680000)
        output
        (-> (test-support/create-test-pipeline)
          (th/apply!
            (th/create game-events)
            #'game.user-score/parse-event
            (th/filter* #'game.hourly-team-score/filter-start-time
              start-min-ts)
            (th/inline
              (fn map-elems* [event]
                [(:user event) (:score event)]))))]
    (-> output
      (PAssert/that)
      (.containsInAnyOrder ^Iterable [["user0_AndroidGreenEchidna" (int 0)]
                                      ["user0_MagentaKangaroo" (int 4)]
                                      ["user2_AmberCockatoo" (int 13)]
                                      ["user18_BananaEmu" (int 7)]
                                      ["user3_BananaEmu" (int 17)]
                                      ["user18_BananaEmu" (int 1)]
                                      ["user18_ApricotCaneToad" (int 14)]]))))