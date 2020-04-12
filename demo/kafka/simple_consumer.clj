(ns kafka.simple-consumer
  (:require [thurber :as th]
            [clojure.tools.logging :as log])
  (:import (org.apache.beam.sdk.io.kafka KafkaIO TimestampPolicyFactory TimestampPolicy TimestampPolicy$PartitionContext KafkaRecord)
           (org.apache.kafka.clients.producer ProducerRecord KafkaProducer)
           (org.apache.kafka.common.serialization ByteArraySerializer Deserializer ByteArrayDeserializer)
           (org.apache.kafka.common TopicPartition)
           (java.util Optional)
           (org.apache.beam.sdk.transforms.windowing BoundedWindow)
           (org.joda.time Instant Duration)
           (org.apache.beam.sdk Pipeline)
           (thurber.java.exp TKafkaDeserializer)
           (org.apache.beam.sdk.values KV)
           (org.apache.beam.sdk.coders StringUtf8Coder)))

;;; Here we demo how to configure a KafkaIO source via Clojure/thurber.

;; To execute this demo you'll need a local Kafka server running -- this is very
;;  easy to set up (see https://kafka.apache.org/quickstart)

;; Kafka needs a Deserializer; we can reify the interface but this instance will
;;   not be serializable. See below how we leverage thurber's TKafkaDeserializer
;;   which can load and delegate to this instance:

(def ^:private deserializer-
  (reify Deserializer
    (deserialize [this ^String topic ^bytes data]
      (when data {::payload (String. data) ::event-ts (Instant.)}))
    (close [this])))

;; A custom TimestampPolicyFactory allows us to control the event timestamps and watermark
;;   used for our Kafka input stream. KafkaIO has out-of-the-box TimestampPolicyFactory if
;;   you do not need finer control. However if you need event timestamps driven off the
;;   actual entities coming from Kafka, then you'll need a custom policy like we have here:

;; TimestampPolicyFactory must be Serializable; Clojure's proxy instances are not Serializable.
;;   See below how we wrap this using `th/proxy*` to achieve Serializabilty:

(def ^:private timestamp-policy-factory
  (proxy [TimestampPolicyFactory] []
    (createTimestampPolicy [^TopicPartition p ^Optional previous-watermark]
      (let [last-seen-event-ts (atom nil)]
        (proxy [TimestampPolicy] []
          (getTimestampForRecord [^TimestampPolicy$PartitionContext context_ ^KafkaRecord record]
            (let [the-ts (or (-> record .getKV .getValue ::event-ts)
                           @last-seen-event-ts BoundedWindow/TIMESTAMP_MIN_VALUE)]
              (swap! last-seen-event-ts
                (fnil
                  (fn [v1 v2]
                    (max-key #(.getMillis %) v1 v2)) BoundedWindow/TIMESTAMP_MIN_VALUE)
                the-ts)))
          (getWatermark [^TimestampPolicy$PartitionContext context_]
            (or @last-seen-event-ts
              (.orElse previous-watermark BoundedWindow/TIMESTAMP_MIN_VALUE))))))))

;; --

(defn- ^Pipeline build-pipeline! [pipeline]
  (doto pipeline
    (th/apply!
      (-> (KafkaIO/read)
        (.withBootstrapServers
          (-> pipeline th/get-custom-config :bootstrap-servers))
        (.withTopic (-> pipeline th/get-custom-config :topic-name))
        (.withConsumerConfigUpdates {"auto.offset.reset" "latest"
                                     "group.id" "thurber/demo/simple-consumer"
                                     ;; This is how we "pass" the delegate Deserializer to thurber's
                                     ;;   TKafkaDeserializer:
                                     "thurber.kafka-value-deserializer" #'deserializer-})
        (.withKeyDeserializer ByteArrayDeserializer)
        (.withValueDeserializerAndCoder TKafkaDeserializer th/nippy)
        (.withTimestampPolicyFactory (th/proxy* #'timestamp-policy-factory))
        (.withoutMetadata))
      (th/fn* ^{:th/coder (StringUtf8Coder/of)} extract-payload [^KV kafka-record]
        (-> kafka-record .getValue ::payload))
      #'th/log-verbose)))

;; Our demo here will generate a few messages to a Kafka topic; our pipeline consumes them
;;    and logs the deserialized element values from Kafka and their event timestamps.

(defn- produce-some-messages! [{:keys [bootstrap-servers topic-name]}]
  (with-open [kafka-producer (KafkaProducer.
                               {"bootstrap.servers" bootstrap-servers
                                "key.serializer" ByteArraySerializer
                                "value.serializer" ByteArraySerializer})]
    (doseq [^String msg ["alpha" "beta" "gamma" "delta" "epsilon"]]
      (->> (ProducerRecord. topic-name (.getBytes msg))
        (.send kafka-producer) .get))))

(defn demo! []
  (let [bootstrap-servers "localhost:9092"
        topic-name "thurber-demo-simple-consumer-1"
        pipeline (-> (th/create-pipeline
                       {:block-on-run false
                        :custom-config {:topic-name topic-name
                                        :bootstrap-servers bootstrap-servers}})
                   build-pipeline!)
        pipeline-result (.run pipeline)]
    (try
      (log/info "waiting for pipeline to start up...")
      (.waitUntilFinish pipeline-result (Duration/standardSeconds 15))
      (log/info "producing some messages...")
      (produce-some-messages! (-> pipeline th/get-custom-config))
      (log/info "waiting for messages to be consumed...")
      (.waitUntilFinish pipeline-result (Duration/standardSeconds 15))
      (finally
        (.cancel pipeline-result)))))
