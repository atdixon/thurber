(ns simple.hello-world
  (:require [thurber :as th]))

(defn- create-pipeline []
  (doto (th/create-pipeline)
    (th/apply!
      (th/create ["hello world"])
      #'th/log-elem*)))

(defn demo! []
  (-> (create-pipeline)
    (.run)))
