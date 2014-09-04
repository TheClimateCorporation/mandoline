(ns io.mandoline.utils
  (:require [clojure.tools.logging :as log]
            [clojure.string :as string]
            [cheshire.core :as json]
            [cheshire.factory :as factory]
            [com.climate.claypoole :as cp]
            [me.raynes.fs :as fs]
            [metrics.timers :refer (timer time!)])
  (:import [java.io IOException]
           [java.util.concurrent Executors Callable Future]
           [com.google.common.io Files]))

(def nthreads 24)
(defonce mandoline-thread-pool (cp/threadpool nthreads))

(defmacro instrument
  "Add a timer to the provided function. The timers are named thusly:
  [namespace function metric]

  You can access the metrics by doing the following:
  (require '[metrics.utils :refer (all-metrics)])
  (all-metrics)"
  [fns]
  `(do
     ~@(for [f fns]
         `(let [ns# (ns-name (:ns (meta (var ~f))))
                nsp# (name ns#)
                n# (name '~f)
                timer# (timer [nsp# n# "time"])]
            (alter-var-root
             #'~f
             (fn [~'func]
               (fn [& ~'args] (time! timer# (apply ~'func ~'args)))))))))

;; FIX: Add a function to add counters as well.

(defn nan?
  "Test whether something is NaN.

  This function may not be correct for exotic floating-point objects.
  However, it works for standard Java numeric types."
  [x]
  (and (float? x) (Double/isNaN x)))

(defn nan=
  "Behaves like clojure.core/= except that is treats NaN as equal to
  NaN."
  ([x] true)
  ([x y]
   (or (= x y) (and (nan? x) (nan? y))))
  ([x y & more]
   (if (nan= x y)
     (if (next more)
       (recur y (first more) (next more))
       (nan= y (first more)))
     false)))

(defn seq1
  "Given a chunked seq s, return a new lazy seq that is not chunked:

  (chunked-seq? s) => true
  (chunked-seq? (seq1 s)) => false

  Code taken from:
  http://blog.fogus.me/2010/01/22/de-chunkifying-sequences-in-clojure/"
  [#^clojure.lang.ISeq s]
  (reify clojure.lang.ISeq
    (first [_] (.first s))
    (more [_] (seq1 (.more s)))
    (next [_] (let [sn (.next s)] (and sn (seq1 sn))))
    (seq [_] (let [ss (.seq s)] (and ss (seq1 ss))))
    (count [_] (.count s))
    (cons [_ o] (.cons s o))
    (empty [_] (.empty s))
    (equiv [_ o] (.equiv s o))))

(def npmap
  "Unlike pmap, we use a local thread pool (IO bounded)."
  (partial cp/pmap mandoline-thread-pool))

(defprotocol VersionFilter
  (committed? [_ version-id]))

(defn- search-version [versions version-id cache]
  (let [found (->> versions
                   (drop-while #(> % version-id))
                   first
                   (= version-id))]
    (swap! cache assoc version-id found)
    found))

;; this dumb implementation assumes that it will not usually need to go
;; very var in the versions lazy-seq. If that's not the case, the first time
;; it needs to do that it will take some time if there are many versions.
;; After that, version-id is cached, so it will be very fast next times
(deftype FullyCachedVersionFilter [cache versions]
  VersionFilter
  (committed? [_ version-id]
    (let [cached (@cache version-id)]
      (if (nil? cached)
        (search-version versions version-id cache)
        cached))))

(defn mk-version-cache [versions]
  (->FullyCachedVersionFilter (atom {}) versions))

(defn generate-metadata
  "This enables the user to write NaNs as NaN representations, instead of a
  \"NaN\" string.  This is safe because we have a corresponding function (below)
  that reads in the NaN as a Double/NaN."
  ([obj]
     (generate-metadata obj nil))
  ([obj opt-map]
     (-> (json/generate-string obj opt-map)
         (string/replace #"\"NaN\"" "NaN"))))

(defn parse-metadata
  ([string] (parse-metadata string nil))
  ([string key-fn] (parse-metadata string key-fn nil))
  ([^String string key-fn array-coerce-fn]
     (binding [factory/*json-factory* (factory/make-json-factory
                                       {:allow-non-numeric-numbers true})]
       (json/parse-string string key-fn array-coerce-fn))))

(defmacro attest
  "Like assert, evaluates expr and throws an IllegalArgumentException if
  it does not evaluate to logical true."
  ([x]
     `(when-not ~x
        (throw (new IllegalArgumentException
                    (str "Attest failed: " (pr-str '~x))))))
  ([x message]
     `(when-not ~x
        (throw (new IllegalArgumentException
                    (str "Attest failed: " ~message "\n" (pr-str '~x)))))))

(defn mk-temp-dir
  "Create a temporary unique directory. Returns the new directory as a File
  object."
  []
  (let [tmp (fs/tmpdir)]
    (when (not (fs/exists? tmp))
      (or (fs/mkdirs tmp)
          (throw (IOException. "Unable to create java.io.tmpdir " tmp))))
    ; Use Guava's createTempDir, me.raynes.fs/temp-dir hangs on some errors
    (Files/createTempDir)))

(defn delete-any
  "Deletes any file or directory.  Directories are recursively deleted."
  [t]
  (if (fs/directory? t)
    (fs/delete-dir t)
    (fs/delete t)))

(defmacro with-deleting
  "Shameless copy of with-open that executes the body in a try block and
  attempts to delete any file or directory bound to symbols.
  Example:
    (with-deleting [tmp (mk-temp-dir)]
      (do-stuff tmp))"
  [bindings & body]
  (cond
    (= (count bindings) 0) `(do ~@body)
    (symbol? (bindings 0)) `(let ~(subvec bindings 0 2)
                              (try
                                (with-deleting ~(subvec bindings 2) ~@body)
                                (finally
                                  (delete-any ~(bindings 0)))))
    :else (throw (IllegalArgumentException.
                   "with-finally only allows symbols in bindings"))))

(comment

  (use 'clojure.test)

  (defn sleepy [x]
    (Thread/sleep 100)
    x)

  (defmacro timeit [& body]
    `(let [s# (System/currentTimeMillis)]
       ~@body
       (- (System/currentTimeMillis) s#)))

  (deftest comment-driven-development
    (testing "npmap"
      (is (= 555 (last (npmap inc (range 555)))))
      (is (> 5
             (- (timeit (dorun (npmap sleepy (range 16))))
                100)))
      (is (> 5
             (- (timeit (dorun (npmap sleepy (range 17))))
                200))))

    (testing "version cache"
      (let [c (mk-version-cache [124 123 100])]
        (is (not (committed? c 99)))
        (is (not (committed? c 150)))
        (is (not (committed? c 101)))
        (is (committed? c 100))
        (is (committed? c 124))
        (is (committed? c 123)))
      (let [c (mk-version-cache [124 123 100])]
        (is (committed? c 123))
        (is (committed? c 100))
        (is (committed? c 124))
        (is (not (committed? c 99)))
        (is (not (committed? c 150)))
        (is (not (committed? c 101))))))

  (comment-driven-development)

  )
