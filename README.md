# thurber

![thurber](img/thurber.png)

[![Clojars Project](https://img.shields.io/clojars/v/com.github.atdixon/thurber.svg)](https://clojars.org/com.github.atdixon/thurber)

[Apache Beam](https://beam.apache.org/) and 
[Google Cloud Dataflow](https://beam.apache.org/get-started/downloads/) on
~~steroids~~ Clojure. The [walkthrough](./demo/walkthrough.clj) explains everything.

_This is alpha software. Always use latest version &amp; watch [release notes](https://github.com/atdixon/thurber/releases) carefully. API subject to mood swings._

* [Quickstart](#quickstart)
* [Project Goals](#project-goals)
* [Documentation](#documentation)
* [Demos](#demos)
    * [Word Count](#word-count)
    * [Mobile Gaming Examples](#mobile-gaming-examples)
* [Make It Fast](#make-it-fast)

## Quickstart

1. Clone &amp; `cd` into this repository.
2. `lein repl`
3. Copy &amp; paste:

```clojure
(ns try-thurber
  (:require [thurber :as th]
            [thurber.sugar :refer :all]))

(->
  (th/create-pipeline)

  (th/apply!
    (read-text-file
      "demo/word_count/lorem.txt")
    (th/fn* extract-words [sentence]
      (remove empty? (.split sentence "[^\\p{L}]+")))
    (count-per-element)
    (th/fn* format-as-text
      [[k v]] (format "%s: %d" k v))
    (log-sink))

  (th/run-pipeline!))
```

Output:

```
...
INFO thurber - extremely: 1
INFO thurber - undertakes: 1
INFO thurber - pleasure: 7
INFO thurber - you: 2
...
```

## Project Goals

* **Enable Clojure**
    * Bring Clojure's powerful, expressive toolkit (destructuring,
      immutability, REPL, async tools, etc etc) to Apache Beam.
* **REPL Oriented**
    * Functions are idiomatic/pure Clojure functions by default. (E.g., lazy
      sequences are supported making iterative event output optional/unnecessary, etc.) 
    * Develop and test pipelines incrementally from the REPL. 
    * Evaluate/learn Beam semantics (windowing, triggering) interactively.
* **Avoid Macros**
    * Limit macro infection. Most thurber constructions are macro-less, use of any
      thurber macro constructions (like inline functions) is optional.
* **AOT Nothing**
    * Fully dynamic experience. Reload namespaces at whim. thurber's dependency on 
      Beam, Clojure, etc versions are completely dynamic/floatable. No forced AOT'd 
      dependencies, Etc.
* **No Lock-in**
    * Pipelines can be composed of Clojure and Java transforms. 
      Incrementally refactor your pipeline to Clojure or back to Java.
* **Not Afraid of Java Interop**
    * Wherever Clojure's [Java Interop](https://clojure.org/reference/java_interop) 
      is performant and works cleanly with Beam's fluent API, encourage it; facade/sugar 
      functions are simple to create and left to your own domain-specific implementations.
* **Completeness**
    * Support all Beam capabilities (Transforms, State &amp; Timers, Side Inputs,
      Output Tags, etc.)
* **Performance**
    * Be finely tuned for data streaming.

## Documentation

* [Code Walkthrough](./demo/walkthrough.clj)
* [Troubleshooting](./doc/troubleshooting.md)
* [Beam Tutorial](./doc/beam-tutorial.md)

## Demos

Each namespace in the `demo/` source directory is a pipeline written in Clojure
using thurber. Comments in the source highlight salient aspects of thurber usage.

Along with the [code walkthrough](./demo/walkthrough.clj) these are the best way to learn 
thurber's API and serve as recipes for various scenarios (use of tags, side inputs,
windowing, combining, Beam's State API, etc etc.)

To execute a demo, start a REPL and evaluate `(demo!)` from within the respective namespace.

### Word Count

The `word_count` package contains ports of Beam's
[Word Count Examples](https://beam.apache.org/get-started/wordcount-example/)
to Clojure/thurber.

### Mobile Gaming Examples

Beam's [Mobile Gaming Examples](https://beam.apache.org/get-started/mobile-gaming-example/)
have been ported to Clojure using thurber.

These are fully functional ports. They require deployment to GCP Dataflow: 

* [How to Run Beam Mobile Gaming Examples](./doc/running-mobile-gaming-examples.md)

## Make It Fast

First make your pipeline work. Then optimize if needed:

* Use Clojure [**type hints**](https://clojure.org/reference/java_interop#typehints) 
liberally within your stream functions. Streaming/big data implies hot code paths.
    - Helpful aliases for certain type hints can be found listed [here](https://clojure.org/reference/java_interop#TypeAliases).
* Use Clojure's [high-performance primitive operations](https://clojure.org/reference/java_interop#primitives) where
  possible.
* See Clojure's [optimization](https://clojure.org/reference/java_interop#optimization) advice.
    - For example, `aget` is explicitly overloaded for primitive arrays and type hinting primitive and Object arrays
      may be essential to get the optimal invocation. 
* Compare [user-score](./demo/game/user_score.clj) with [user-score-opt](./demo/game/user_score_opt.clj) in the 
  [demo](./demo) directory for what an optimization pass might look like.
    - This optimized version compares with the performance of the same Java demo implemented by Beam.
* Be explicit which JVM/JDK runtime version is executing your code. More mature JVM versions typically have stronger
  performance enhancements than earlier versions.
    - Dataflow, for example, will pick a JVM/JDK version for worker node runtimes based on the Java version you
      use to launch your pipeline.
* Profile, profile, profile.
    - If deploying to GCP, use [Dataflow profiling](https://medium.com/google-cloud/profiling-dataflow-pipelines-ddbbef07761d)
to zero in on areas to optimize.
* Fallback to Java for sensitive code, if unsure. (This rarely if ever should be needed to achieve optimal performance.)
* In general (this is not Clojure/thurber-specific) you should understand Beam "fusion" and when to break fusion to achieve 
  greater linear scalability. More info [here](https://beam.apache.org/contribute/ptransform-style-guide/#performance).
    
NOTE aggressive optimizations may buy you some bottom-line cost improvements. Beam achieves linear scalability and
often the slight overhead of clean Clojure code trumps the cost-savings of an aggressive optimization. Aggressive 
optimizations of Beam jobs is more likely to save you bottom-line resource cost over throughput/latency so keep this
in mind as you prioritize dev/testing cost &amp; effort of aggressive tuning efforts. 

## References

* https://write.as/aaron-d/clojure-data-streaming-and-dodging-static-types

## License
Copyright © 2020 Aaron Dixon

Like Clojure distributed under the Eclipse Public License.
