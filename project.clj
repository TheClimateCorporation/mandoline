(defproject io.mandoline/mandoline-core "0.1.3"
  :description
    "Mandoline is a distributed store for multi-dimensional arrays"
  :license {:name "Apache License, version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"
            :distribution :repo}
  :min-lein-version "2.0.0"
  :url
    "https://github.com/TheClimateCorporation/mandoline"
  :mailing-lists
    [{:name "mandoline-users@googlegroups.com"
      :archive "https://groups.google.com/d/forum/mandoline-users"
      :post "mandoline-users@googlegroups.com"}
     {:name "mandoline-dev@googlegroups.com"
      :archive "https://groups.google.com/d/forum/mandoline-dev"
      :post "mandoline-dev@googlegroups.com"}]
  :resource-paths ["resources"]
  :checksum :warn
  :dependencies
    [[org.clojure/clojure "1.5.1"]
     [org.slf4j/slf4j-log4j12 "1.7.2"]
     [log4j "1.2.17"]
     [org.clojure/tools.logging "0.2.6"]
     [metrics-clojure "1.0.1"]

     ;; core library
     [org.clojure/core.cache "0.6.3"]
     [joda-time/joda-time "2.1"]
     [commons-codec "1.6"]
     [cheshire "5.1.1"]
     [edu.ucar/netcdf "4.2.20"]
     [org.clojure/math.combinatorics "0.0.4"]
     [org.clojure/math.numeric-tower "0.0.2"]
     [me.raynes/fs "1.4.3"]
     [com.google.guava/guava "15.0"]

     ;; nrepl to launch a remote repl
     [org.clojure/tools.nrepl "0.2.3"]

     ;; Filters
     [net.jpountz.lz4/lz4 "1.1.2"]

     ;; Test helpers
     [org.clojure/tools.cli "0.3.1"]
     [org.clojure/test.check "0.5.7"]]
  :exclusions [org.clojure/clojure]

  :profiles {
    :dev {:dependencies
           [[org.apache.commons/commons-lang3 "3.1"]
            ;; Clojure library for Java MessageDigest hashing
            [digest "1.4.3"]
            [midje "1.5.1"]
            [org.clojure/tools.namespace "0.2.4"]]
          :plugins [[lein-test-out "0.3.0"]]}}

  :aliases {"docs" ["marg" "-d" "target"]
            "package" ["do" "clean," "jar"]}

  :uberjar-name "mandoline.jar"
  :plugins [[lein-marginalia "0.7.1"]
            [lein-cloverage "1.0.2"]]
  :test-selectors {:default (fn [v] (and (not (:manual v)) (not (:experimental v))))
                   :integration :integration
                   :manual :manual
                   :experimental :experimental
                   :all (fn [v] (not (:experimental v)))})
