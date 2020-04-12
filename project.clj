(def version-apache-beam "2.19.0")

(defproject com.github.atdixon/thurber "0.0.5-alpha3-SNAPSHOT"
  :author "Aaron Dixon <https://write.as/aaron-d/>"
  :description "thurber: Apache Beam on Clojure"
  :url "https://github.com/atdixon/thurber"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo}

  :deploy-repositories [["releases" {:url "https://repo.clojars.org" :creds :gpg}]
                        ["snapshots" {:url "https://repo.clojars.org" :creds :gpg}]]

  :java-source-paths ["src"]
  :javac-options ["-target" "1.8" "-source" "1.8"]

  :pedantic? false

  :dependencies
  [[camel-snake-kebab "0.4.1"]
   [clj-time "0.15.2"]
   [org.clojure/clojure "1.10.1"]
   [org.clojure/data.json "0.2.7"]
   [org.clojure/tools.logging "0.5.0"]
   [org.apache.beam/beam-sdks-java-core ~version-apache-beam]
   [com.google.code.findbugs/jsr305 "3.0.2"]
   [org.javassist/javassist "3.26.0-GA"]
   [com.taoensso/nippy "2.14.0"]
   [org.apache.kafka/kafka-clients "2.4.1"]
   [org.slf4j/slf4j-api "1.7.29"]]

  :profiles
  {:demo {:source-paths ["demo"]
          :dependencies
          [;; -- demo deps --
           [org.clojure/core.async "0.7.559"]
           [deercreeklabs/lancaster "0.8.6"]
           [org.apache.beam/beam-runners-direct-java ~version-apache-beam]
           [org.apache.beam/beam-examples-java ~version-apache-beam
            :exclusions [org.slf4j/slf4j-jdk14]]
           [org.slf4j/slf4j-simple "1.7.29"]]}
   :dev [:demo
         :extra
         :experimental
         {:source-paths ["demo"]
          :dependencies
          [;; -- test deps --
           [org.hamcrest/hamcrest-core "2.2"]
           [org.hamcrest/hamcrest-library "2.2"]
           ;; -- benchmarking --
           [criterium "0.4.5"]]
          :plugins
          [[lein-pprint "1.2.0"]
           [lein-ancient "0.6.15"]
           [lein-codox "0.10.7"]]}]
   :extra {:dependencies
           [[org.apache.beam/beam-sdks-java-io-kafka ~version-apache-beam]
            [org.apache.beam/beam-sdks-java-io-jdbc ~version-apache-beam]
            [org.apache.beam/beam-sdks-java-io-amazon-web-services ~version-apache-beam]]}
   :experimental {:dependencies []}
   :staging-repos {:repositories [["repository.apache.org/staging"
                                   {:url
                                    "https://repository.apache.org/content/repositories/orgapachebeam-1101"
                                    ;; "https://repository.apache.org/content/groups/staging/"
                                    }]]}
   :dataflow {:dependencies
              [[org.apache.beam/beam-sdks-java-io-google-cloud-platform ~version-apache-beam]
               [org.apache.beam/beam-runners-google-cloud-dataflow-java ~version-apache-beam]]}})
