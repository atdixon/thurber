(ns game.user-score-opt
  (:require [thurber :as th]
            [clojure.tools.logging :as log]
            [deercreeklabs.lancaster :as lan])
  (:import (org.apache.beam.sdk.io TextIO)
           (org.apache.beam.sdk.values KV)
           (org.apache.beam.sdk.transforms Sum)
           (thurber.java TCoder)
           (org.apache.beam.sdk.coders CustomCoder KvCoder StringUtf8Coder VarIntCoder)
           (java.io OutputStream InputStream)
           (java.nio ByteBuffer)))

;;
;; Optimization #1: To minimize payload of message bytes (and therefore storage demands for the batch or
;;    streaming thurber job, we will use Avro for ser/de instead of Nippy.
;;
;;    Nippy will happily ser/de defrecord types; each message payload will contain the full classname
;;    of the defrecord as overhead. In many cases, especially for even types with many fields, simply using
;;    nippy plus defrecord will be a sufficient optimization.
;;
;;    For payload with few fields a simple Clojure map will beat the defrecord serialization.
;;
;;    Avro is the most optimal choice as neither field names nor a type name needs to be encoded
;;    in each payload.
;;

(defrecord GameActionInfo [user team score timestamp])

(lan/def-record-schema game-action-info-schema
  [:user lan/string-schema]
  [:team lan/string-schema]
  [:score lan/int-schema]
  [:timestamp lan/long-schema])

(def ^:private game-action-info-coder-impl
  (proxy [CustomCoder] []
    (encode [val ^OutputStream out]
      (let [record-bytes ^"[B" (lan/serialize game-action-info-schema val)
            size (count record-bytes)]
        (.write out (-> (ByteBuffer/allocate 4) (.putInt size) (.array)))
        (.write out record-bytes)))
    (decode [^InputStream in]
      (let [size-bytes (byte-array 4)
            _ (.read in size-bytes)
            size (.getInt (ByteBuffer/wrap size-bytes))
            record-bytes (byte-array size)
            _ (.read in record-bytes)]
        (lan/deserialize-same game-action-info-schema record-bytes)))))

(def game-action-info-coder
  (TCoder. #'game-action-info-coder-impl))

(defn- ^{:th/coder game-action-info-coder} parse-event [^String elem]
  (try
    ;; Optimization #2: Use low-level primitive array operations on a type-hinted array to avoid
    ;;    overhead with Clojure's polymorphic suboptimal aget, etc.
    (let [^"[Ljava.lang.Object;" parts (.split elem "," -1)]
      (if (>= (alength parts) 4)
        (->GameActionInfo
          ;; Optimization #2/a: clojure.core/aget here needs the array type hint above to pick the optimal
          ;;    primitive invocation!
          (.trim ^String (aget parts 0))
          (.trim ^String (aget parts 1))
          (Integer/parseInt (.trim ^String (aget parts 2)))
          (Long/parseLong (.trim ^String (aget parts 3))))
        (log/warnf "parse error on %s, missing part" elem)))
    (catch NumberFormatException e
      (log/warnf "parse error on %s, %s" elem (.getMessage e)))))

(def ^:private kv-string-int-coder
  (KvCoder/of (StringUtf8Coder/of) (VarIntCoder/of)))

(defn- ^{:th/coder kv-string-int-coder} ->field-and-score-kv [field elem]
  (KV/of (field elem) (:score elem)))

(defn ->extract-sum-and-score-xf [field]
  (th/compose "extract-sum-and-score"
    (th/partial #'->field-and-score-kv field)
    (Sum/integersPerKey)))

(defn- ->write-to-text-xf [output row-formatter]
  (th/compose "write-to-text"
    row-formatter
    (-> (TextIO/write)
      (.to ^String output))))

(defn- create-pipeline [opts]
  (let [pipeline (th/create-pipeline opts)
        conf (th/get-custom-config pipeline)]
    (doto pipeline
      (th/apply!
        (-> (TextIO/read)
          (.from ^String (:input conf)))
        #'parse-event
        (->extract-sum-and-score-xf :user)
        (->write-to-text-xf (:output conf)
          ;; Optimization #3: Use explicit String coder where we know we have Strings,
          ;;    instead of default nippy coder.
          (th/fn* ^{:th/coder (StringUtf8Coder/of)} format-row [^KV kv]
            (format "user: %s, total_score: %d" (.getKey kv) (.getValue kv))))))))

(defn demo! [& args]
  (-> (create-pipeline
        (concat
          args
          (th/->beam-args
            {:custom-config
             {:input "gs://apache-beam-samples/game/gaming_data*.csv"
              :output "gs://thurber-demo/user-score-opt-"}})))
    (.run)))
