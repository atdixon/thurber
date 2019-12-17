# thurber

(alpha release coming soon)

[Apache Beam](https://beam.apache.org/) and 
[Google Cloud Dataflow](https://beam.apache.org/get-started/downloads/) on
~~steroids~~ Clojure.

* [Principles](#principles)
* [Quickstart](#quickstart)
* [Guide](#guide)
    * [Transforms](#transforms)
    * [Coders](#coders)
* [Demos](#demos)
    * [Word Count](#word-count)
    * [Mobile Gaming Example](#mobile-gaming-example)
* [Make It Fast](#make-it-fast)

## Principles

* **Enable Clojure**
    * Bring Clojure's powerful, expressive tookit (destructuring,
      immutability, REPL, async tools, etc etc) to Apache Beam.
* **REPL Friendly**
    * Build and test your pipelines incrementally in the REPL. 
    * Learn Beam semantics (windowing, triggering) interactively. 
* **No Macros**
* **No AOT**
* **Bypassable API**
    * No API lock-in. Pipelines can be composed of Clojure/thurber and Java 
      transforms. Don't like **thurber**? Incrementally refactor your pipeline
      back to Java.
* **Not Afraid of Java Interop**
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

## Guide

Beam's Java API is oriented around constructing a [Pipeline](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/Pipeline.html)
and issuing a series of `.apply` invocations to mutate the pipeline and build up a directed, acyclic graph of stages.

`thurber/apply!` applies a series of transforms 
to a `Pipeline` or [PCollection](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/values/PCollection.html). 
(When pipeline graphs fork, multiple `apply!` invocations may be needed to create the different paths.)

`thurber/comp*` is how [composite transforms](https://beam.apache.org/documentation/programming-guide/#composite-transforms)
are made. The result of `comp*` is a transform that can be used again within a subsequent call to `apply!` or `comp*`.

#### Transforms

Any [PTransform](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/PTransform.html) 
instance can be provided to `apply!` and `comp*` but thurber supports other transform representations:

* A Clojure var can be provided and will convert to a 
  [ParDo](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/ParDo.html) 
  transform. The var must ref a function:
    * e.g., `#'extract-words`
    * The will be invoked with the processing element as its arg.
    * The return value of the function will automatically emit downstream via 
  [ProcessContext.output()](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.WindowedContext.html#output-OutputT-).
    * If the function returns a `seq` then all items in the seq are emitted. Often these seqs are lazy and produce
  many elements.
    * May return `nil` in which case no automatic output occurs.
    * Some complex `ParDo` implementations will need first-class access to the current 
      [ProcessContext](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.WindowedContext.html)
      instance.
        * `thurber/*process-context*` is always bound to this current context.
        * When emitting elements _explicitly_ via this bound var, functions will typically return `nil`
          so that no automatic emissions occur.
* `thurber/partial*`, `filter*`, `combine-per-key`, and other thurber functions produce transform 
  representations that can be used in `apply!` and `comp*`

#### Coders

Every Beam `PCollection` must have a
[Coder](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/coders/Coder.html).

The `thurber/nippy` coder de/serializes [nippy](https://github.com/ptaoussanis/nippy) supported data types 
(this includes all Clojure core data types).

`thurber/nippy-kv` codes Beam 
[KV](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/values/KV.html)
values that contain nippy-supported data types for the key and value.

Coders can be specified as Clojure metadata on function vars, or directly 
within a thurber transformation representation.

The default coder used is `thurber/nippy` which is appropriate for var-referenced
functions (`ParDo`) that output Clojure data types.

## Demos

Each namespace in the `demo/` source directory is a pipeline written in Clojure
using thurber. Comments in the source highlight salient aspects of thurber usage.

These are the best way to learn thurber's API and serve as recipes for
various scenarios (use of tags, side inputs, windowing, combining,
Beam's State API, etc etc.)

To execute a demo, start a REPL and evaluate `(demo!)` from within the respective namespace.

### Word Count

The `word_count` package contains ports of Beam's
[Word Count Examples](https://beam.apache.org/get-started/wordcount-example/)
to Clojure/**thurber**.

### Mobile Gaming Example

Beam's [Mobile Gaming Examples](https://beam.apache.org/get-started/mobile-gaming-example/)
have been ported to Clojure using **thurber**.

These are fully functional ports but require deployment to GCP Dataflow. (How-to 
notes coming soon.)

## Make It Fast

First make your pipeline work. Then make it fast. Streaming/big data implies hot code paths.
Use Clojure [type hints](https://clojure.org/reference/java_interop#typehints) liberally.

## License
Copyright © 2020 Aaron Dixon

Like Clojure distributed under the Eclipse Public License.
