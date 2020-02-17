(ns simple.view-as-map
  (:require [thurber :as th])
  (:import (org.apache.beam.sdk.transforms View Combine SerializableBiFunction)
           (org.apache.beam.sdk.values KV)
           (java.util HashMap Map)
           (org.apache.beam.sdk.coders MapCoder)))

;; Scenario -- we have a data stream with metadata (light "wavelengths") on some
;; elements in our stream but not all; we use a Beam View to capture a map of
;; wavelength values per key and annotate the stream elements where the metadata
;; is missing.

;; The main demonstration here is an implementation of "view-as-clj-map"; Beam's
;; default Views yield Java data structures. We could produce a Beam/Java Map
;; view and then convert it to a clojure map each element but this would be
;; too much redundant computation.

;; (Note: custom Clojurey views may become part of thurber API ultimately.)

(defn- has-wavelength? [^KV elem]
  (-> elem .getValue :wavelength-nm some?))

(defn- ^{:th/coder th/nippy-kv} ->k-wavelength-val [^KV elem]
  (KV/of (.getKey elem) (-> elem .getValue :wavelength-nm)))

(defn- ^{:th/coder th/nippy} augment-element [map-view ^KV elem]
  (if (has-wavelength? elem)
    elem
    (let [^Map m (.sideInput (th/*process-context) map-view)
          [k v] (th/kv->clj* elem)]
      (KV/of k (assoc v :wavelength-nm (m k))))))

(def view-as-clj-map-xf
  (th/compose "view-as-clj-map"
    {:th/coder th/nippy
     :th/xform
     (Combine/globally
       (th/combiner
         (th/fn* ^{:th/coder th/nippy} view-as-map-extractf [acc]
           (into {} acc))
         (th/fn* view-as-map-combinef [& accs]
           (reduce (fn [memo acc]
                     (doto ^Map memo
                       (.putAll ^Map acc)))
             (HashMap.) accs))
         (th/fn* ^{:th/coder (MapCoder/of th/nippy th/nippy)} view-as-map-reducef
           ([] (HashMap.))
           ([acc ^KV x] (do
                          (when (contains? acc (.getKey x))
                            (throw (IllegalArgumentException.
                                     (format "Duplicate values for: %s" (.getKey x)))))
                          (doto acc
                            (.put (.getKey x) (.getValue x))))))))}
    (View/asSingleton)))

(defn- build-pipeline! [pipeline]
  (let [data (th/apply! pipeline
               (th/create
                 [[:red {}]
                  [:red {:wavelength-nm 680}]
                  [:red {}]
                  [:blue {:wavelength-nm 380}]
                  [:blue {}]])
               #'th/clj->kv)
        map-view (th/apply! data
                   (th/filter #'has-wavelength?)
                   #'->k-wavelength-val
                   (Combine/perKey
                     ^SerializableBiFunction
                     (th/ser-fn #'min))
                   view-as-clj-map-xf)]
    (th/apply!
      data
      (th/partial #'augment-element map-view)
      #'th/log-verbose)
    pipeline))

(defn demo! []
  (-> (th/create-pipeline) build-pipeline! .run))

