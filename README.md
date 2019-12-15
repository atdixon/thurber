# thurber


A thin, muscular Clojure API for [Apache Beam](https://beam.apache.org/) and 
[Google Cloud Dataflow](https://beam.apache.org/get-started/downloads/).

* [Principles](#principles)
* [Quickstart](#quickstart)
* [Demos](#demos)
    * [Mobile Gaming Example](#mobile-gaming-example)
* [Make It Fast](#make-it-fast)

## Principles

* Enable Clojure
    * Bring Clojure's powerful, expressive tookit (destructuring,
      immutability, REPL, async tools, etc etc) to Apache Beam.
* REPL Friendly
    * Build and test your pipelines incrementally in the REPL. 
    * Learn Beam semantics (windowing, triggering) interactively. 
* No Macros
* No AOT
* Bypassable API
    * No API lock-in. Pipelines can be composed of Clojure/thurber and Java 
      transforms. Don't like **thurber**? Incrementally refactor your pipeline
      back to Java.
* Not Afraid of Java Interop
    * Wherever Clojure's [Java Interop](https://clojure.org/reference/java_interop) works 
      cleanly with Beam, embrace it.

## Quickstart

1. Clone &amp; `cd` into this repository.
2. `lein repl`
3. Copy &amp; paste:

```clojure

(ns try-thurber
   (:require [thurber :as th]
             [clojure.string :as str])
   (:import (org.apache.beam.sdk.io TextIO)))

(defn- extract-words [sentence]
  (remove empty? (str/split sentence #"[^\p{L}]+")))

(defn- format-as-text [[k v]]
  (format "%s: %d" k v))

(.run
    (doto (th/create-pipeline)
      (th/apply!
        (-> (TextIO/read)
          (.from "demo/word_count/lorem.txt"))
        #'extract-words
        #'th/->kv
        (th/count-per-key)
        #'format-as-text
        #'th/log-elem*)))

```

## Demos

Each namespace in `demo/` source directory is a pipeline written in Clojure
using **thurber**. Comments within the source.

These are the best way to learn **thurber**'s API and serve as recipes for
various scenarios (use of tags, side inputs, windowing, combining,
Beam's State API, etc etc.)

To execute a demo, start a REPL and run `(demo!)` in the respective namespace.

The `word_count` package contains ports of Beam's
[Word Count Examples](https://beam.apache.org/get-started/wordcount-example/)
to Clojure/**thurber**.

### Mobile Gaming Example

Beam's [Mobile Gaming Examples](https://beam.apache.org/get-started/mobile-gaming-example/)
have been ported to Clojure using **thurber**.

These are fully functional ports but require deployment to GCP Dataflow. (How-to 
notes coming soon.)

## Make It Fast

First, make your pipeline work. Then make it fast.

Streaming/big data implies hot code paths.

Use Clojure [type hints](https://clojure.org/reference/java_interop#typehints)
freely.

(More here soon.)

## License
Copyright © 2020 Aaron Dixon

Like Clojure distributed under the Eclipse Public License.
