(ns simple.multi-out
  (:require [thurber :as th]
            [clojure.tools.logging :as log]))

(defn- sink* [prefix elem]
  (log/infof "[%s] %s" prefix elem))

(defn- ^{:th/output-tags [:square :dec :inc]} splay* [val]
  [:square (* val val)
   :dec (dec val)
   :inc (inc val)])

(defn- create-pipeline []
  (let [pipeline (th/create-pipeline)
        data (th/apply! pipeline (th/create* [1 2 3 4 5]))]
    (th/apply!
      data
      #'splay*
      #'sink*)
    pipeline))

(defn demo! []
  (-> (create-pipeline)
    (.run)))

