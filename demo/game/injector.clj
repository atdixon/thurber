(ns game.injector
  (:import (org.apache.beam.examples.complete.game.injector Injector)
           (java.lang.reflect Method)))

(def ^:private ^Class class-str-array
  (Class/forName "[Ljava.lang.String;"))

(def ^:private ^Method method-injector-main
  (doto
    (.getMethod Injector "main"
      (into-array Class [class-str-array]))
    (.setAccessible true)))

(defn -main [& args]
  (.invoke method-injector-main nil
    (into-array Object [(into-array String args)])))
