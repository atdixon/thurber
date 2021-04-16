(def version-apache-beam "2.28.0")

(defproject com.github.atdixon/thurber "1.1.0-SNAPSHOT"
  :author "Aaron Dixon <https://write.as/aaron-d/>"
  :description "thurber: Apache Beam on Clojure"
  :url "https://github.com/atdixon/thurber"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo}

  :repositories [["confluent" {:url "https://packages.confluent.io/maven/"}]]

  :deploy-repositories [["releases" {:url "https://repo.clojars.org" :creds :gpg}]
                        ["snapshots" {:url "https://repo.clojars.org" :creds :gpg}]]

  :java-source-paths ["src"]
  :javac-options ["-target" "1.8" "-source" "1.8"]

  :pedantic? false

  :dependencies
  [[camel-snake-kebab "0.4.2"]
   [clj-time "0.15.2"]
   [org.clojure/clojure "1.10.3"]
   [org.clojure/data.json "2.1.1"]
   [org.clojure/tools.logging "1.1.0"]
   [org.apache.beam/beam-sdks-java-core ~version-apache-beam]
   [com.google.code.findbugs/jsr305 "3.0.2"]
   [org.javassist/javassist "3.27.0-GA"]
   [com.taoensso/nippy "2.14.0"]
   [org.apache.kafka/kafka-clients "2.7.0"]
   [org.slf4j/slf4j-api "1.7.30"]]

  :profiles
  {:demo {:source-paths ["demo"]
          :dependencies
          [;; -- demo deps --
           [org.clojure/core.async "1.3.610"]
           [deercreeklabs/lancaster "0.9.8"]
           [org.apache.beam/beam-runners-direct-java ~version-apache-beam]
           [org.apache.beam/beam-examples-java ~version-apache-beam
            :exclusions [org.slf4j/slf4j-jdk14]]
           [org.slf4j/slf4j-simple "1.7.30"]]}
   :dev [:demo
         :extra
         {:source-paths ["demo"]
          :dependencies
          [;; -- test deps --
           [org.hamcrest/hamcrest-core "2.2"]
           [org.hamcrest/hamcrest-library "2.2"]
           ;; -- benchmarking --
           [criterium "0.4.6"]]
          :plugins
          [[lein-pprint "1.2.0"]
           [lein-ancient "0.6.15"]
           [lein-codox "0.10.7"]]}]
   :extra {:dependencies
           [[org.apache.beam/beam-sdks-java-io-kafka ~version-apache-beam]
            [org.apache.beam/beam-sdks-java-io-jdbc ~version-apache-beam]]}
   :extra-unused {:dependencies
                  [[org.apache.beam/beam-sdks-java-io-amazon-web-services ~version-apache-beam]]}
   :staging-repos {:repositories [["repository.apache.org/staging"
                                   {:url
                                    "https://repository.apache.org/content/repositories/orgapachebeam-1110"
                                    ;; "https://repository.apache.org/content/groups/staging/"
                                    }]]}
   :dataflow {:dependencies
              [[org.apache.beam/beam-sdks-java-io-google-cloud-platform ~version-apache-beam]
               [org.apache.beam/beam-runners-google-cloud-dataflow-java ~version-apache-beam]]}})
