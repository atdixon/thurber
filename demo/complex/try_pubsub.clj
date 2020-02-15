(ns complex.try-pubsub
  (:require [thurber :as th])
  (:import (org.apache.beam.sdk.io.gcp.pubsub PubsubIO)
           (io.grpc ManagedChannelBuilder ManagedChannel)
           (com.google.api.gax.grpc GrpcTransportChannel)
           (com.google.api.gax.rpc FixedTransportChannelProvider TransportChannelProvider)
           (com.google.cloud.pubsub.v1 TopicAdminClient TopicAdminSettings Publisher)
           (com.google.api.gax.core NoCredentialsProvider CredentialsProvider)
           (com.google.pubsub.v1 ProjectTopicName)))

(def ^:private ^String emulator-project-name "thurber-demo-local")
(def ^:private ^String emulator-topic-name "thurber-demo-local-topic")

;; --

(defn- build-pipeline! [pipeline]
  (doto pipeline
    (th/apply!
      #_(th/create ["hello world"])
      (-> (PubsubIO/readStrings))
      #'th/log)))

(defn demo! []
  (-> (th/create-pipeline) build-pipeline! .run))

;; --

;; gcloud beta emulators pubsub start --project=thurber-demo-local

;; @see https://cloud.google.com/pubsub/docs/emulator#pubsub-emulator-java

(defn- ^TransportChannelProvider create-channel-provider [^ManagedChannel channel]
  (FixedTransportChannelProvider/create
    (GrpcTransportChannel/create channel)))

(defn- ^CredentialsProvider create-credentials-provider []
  (NoCredentialsProvider/create))

(defn- ^TopicAdminClient create-topic-admin-client [^TransportChannelProvider chan-provider
                                                    ^CredentialsProvider creds-provider]
  (TopicAdminClient/create
    ^TopicAdminSettings
    (-> (TopicAdminSettings/newBuilder)
      (.setTransportChannelProvider chan-provider)
      (.setCredentialsProvider creds-provider)
      .build)))

(defn ^Publisher create-publisher [^TransportChannelProvider chan-provider
                                   ^CredentialsProvider creds-provider
                                   ^String topic-name]
  (-> (Publisher/newBuilder topic-name)
    (.setChannelProvider chan-provider)
    (.setCredentialsProvider creds-provider)))

(defn- setup! []
  (let [hostport (or (System/getenv "PUBSUB_EMULATOR_HOST") "localhost:8085")
        channel (-> (ManagedChannelBuilder/forTarget hostport) .usePlaintext .build)]
    (try
      (let [topic-name (ProjectTopicName/of emulator-project-name emulator-topic-name)
            chan-provider (create-channel-provider channel)
            creds-provider (create-credentials-provider)
            client (create-topic-admin-client chan-provider creds-provider)]
        (.createTopic client topic-name))
      (finally
        (.shutdown channel)))))