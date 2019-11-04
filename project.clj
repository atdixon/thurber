(defproject com.github.atdixon/thurber "0.1.0-alpha"
  :author "Aaron Dixon <https://write.as/aaron-d/>"
  :description "Apache Beam on Steroids aka Clojure"
  :url "https://github.com/atdixon/thurber"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo}

  :java-source-paths ["src"]
  :javac-options ["-target" "1.8" "-source" "1.8"]

  :dependencies
  [[camel-snake-kebab "0.4.0"]
   [clj-time "0.15.2"]
   [com.taoensso/nippy "2.14.0"]
   [org.clojure/data.json "0.2.6"]
   [org.clojure/clojure "1.10.1"]
   [org.clojure/core.async "0.4.500"]
   [org.clojure/tools.logging "0.5.0"]
   [org.slf4j/slf4j-api "1.7.29"]
   [org.apache.kafka/kafka-clients "1.0.0"]
   [org.apache.beam/beam-sdks-java-core "2.16.0"]
   [org.apache.beam/beam-sdks-java-io-kafka "2.16.0"]]

  :profiles
  {:dev {:plugins
         [[lein-pprint "1.2.0"]
          [lein-ancient "0.6.15"]
          [lein-codox "0.10.7"]]}
   :demo {:source-paths ["src" "demo"]
          :dependencies
          [[org.apache.beam/beam-runners-direct-java "2.16.0"]
           [org.slf4j/slf4j-simple "1.7.29"]]}
   :dataflow {:dependencies
              [[org.apache.beam/beam-sdks-java-io-google-cloud-platform "2.16.0"]
               [org.apache.beam/beam-runners-google-cloud-dataflow-java "2.16.0"]]}}

  :aliases
  {"deploy-lib" ["do" "deploy" "clojars," "install"]}

  :repositories {"sonatype-oss-public"
                 "https://oss.sonatype.org/content/groups/public/"})
