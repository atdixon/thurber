(ns simple.side-map
  (:require [thurber :as th])
  (:import (org.apache.beam.sdk.transforms Mean View Combine SerializableBiFunction)
           (org.apache.beam.sdk.values KV PCollection)
           (java.util Map)
           (thurber.java MutableTransientHolder)))

(defn- has-wavelength? [^KV elem]
  (-> elem .getValue :wavelength-nm some?))

(defn- ^{:th/coder th/nippy-kv} ->k-wavelength-val [^KV elem]
  (KV/of (.getKey elem) (-> elem .getValue :wavelength-nm)))

(defn- ^{:th/coder th/nippy} augment-element [map-view ^KV elem]
  (if (has-wavelength? elem)
    elem
    (let [m (.sideInput (th/*process-context) map-view)
          [k v] (th/kv->clj* elem)]
      (KV/of k (assoc v :wavelength-nm (m k))))))

(def view-as-map-xf
  (th/compose "view-as-map"
    {:th/coder th/nippy
     :th/xform (Combine/globally
                 (th/combiner
                   (th/fn* view-as-map-extractf [^MutableTransientHolder x]
                     (.asFinalPersistent x))
                   (th/fn* view-as-map-combinef [& accs]
                     (MutableTransientHolder.
                       (reduce (fn [memo acc]
                                 (reduce (fn [res [k v]]
                                           (assoc! res k v))
                                   memo acc))
                         (.-held ^MutableTransientHolder (first accs))
                         (map #(-> ^MutableTransientHolder % .asFinalPersistent) (rest accs)))))
                   (th/fn* view-as-map-reducef
                     ([] (MutableTransientHolder. (transient {})))
                     ([^MutableTransientHolder acc ^KV x] (do (assoc! (.-held acc) (.getKey x) (.getValue x)) acc)))))}
    (View/asSingleton)))

;; todo now have to figure out TCombine CODERS
;; todo [direct-runner-worker] WARN org.apache.beam.sdk.util.MutationDetectors - Coder of type class org.apache.beam.sdk.coders.KvCoder has a #structuralValue method which does not return true when the encoding of the elements is equal. Element KV{null, thurber.java.MutableTransientHolder@73176493}

(defn- build-pipeline! [pipeline]
  (let [data (th/apply! pipeline
               (th/create
                 [[:red {:wavelength-nm 680}]
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
                   view-as-map-xf)]
    (th/apply!
      data
      (th/partial #'augment-element map-view)
      #'th/log-verbose)
    pipeline))

(defn demo! []
  (-> (th/create-pipeline) build-pipeline! .run))

