# thurber

(alpha release coming soon)

A thin, muscular Clojure API for [Apache Beam](https://beam.apache.org/) and 
[Google Cloud Dataflow](https://beam.apache.org/get-started/downloads/).

* [Principles](#principles)
* [Quickstart](#quickstart)
* [Guide](#guide)
    * `thurber/apply!`
    * `thurber/comp*`
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

#### `thurber/apply!`

Apply a series of transforms.

The first arg is a [Pipeline](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/Pipeline.html)
or something else that can have a [PTransform](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/PTransform.html)
applied (eg, [PCollection](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/values/PCollection.html), 
[PBegin](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/values/PBegin.html), ...)

Subsequent args are `PTransform` representations. These can be actual instances
of [PTransform](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/PTransform.html) 
or thurber representations of transforms.

The result is a side-effect: the first arg (often, a pipeline) is
mutated to include the applied transforms

#### `thurber/comp*`

Compose a series of transforms.

The first arg is an optional name (string) for the Beam
[composite transform](https://beam.apache.org/documentation/programming-guide/#composite-transforms).
 
Subsequent args are `PTransform` representations.

The result is a composite `PTransform` composed of the provided
transforms. This transform can be used like any other transform;
i.e., in `thurber/apply!` or inside another `th/comp*`.

#### Transforms

##### [ParDo](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/ParDo.html)

Beam transforms must be `Serializable`. Not all Clojure functions are
readily serializable but Clojure vars are.

Any Clojure var referring to a function can be used in a call to
`thurber/apply!` or `thurber/comp*`. 

The function will be treated as a `ParDo`. By default the referent function will be provided the 
streaming [element](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.ProcessContext.html#element--)
as its first arg. 

(`thurber/partial*` can be used to provide serializable
preceding arguments, often pipeline config values, to the function.)

Any Clojure `seq` can be returned from the function; often this is a lazy
seq. The values in the seq will each be emitted downstream via Beam's
[output](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.WindowedContext.html#output-OutputT-)
method.

Alternatively a *single* non-seq value can be returned from the function
and will be emitted downstream.

A nil value returned will cause no automatic downstream emissions.

`thurber/*process-context*` is always bound to the current
[ProcessContext](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.ProcessContext.html) 
and can be referenced to make explicit invocations on this object.

#### Coders

(todo)

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
