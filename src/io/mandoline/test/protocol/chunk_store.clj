(ns io.mandoline.test.protocol.chunk-store
  "Test helpers for testing implementations of the Mandoline ChunkStore
  protocol."
  (:require
    [clojure.test :refer :all]
    [clojure.test.check.generators :as gen]
    [io.mandoline.impl.protocol :as proto])
  (:import
    [java.nio ByteBuffer]
    [org.apache.commons.codec.binary Hex]))

(def ^:private hexadecimal-chars
  [\0 \1 \2 \3 \4 \5 \6 \7 \8 \9 \a \b \c \d \e \f])

(defn- zip
  [& colls]
  (apply map vector colls))

(defn- generate-non-strings
  "Create a finite seq of pseudorandom non-string values."
  [n]
  (let [max-size 4]
    (take n (gen/sample-seq
              (gen/such-that (complement string?) gen/any)
              max-size))))

(defn- generate-non-integers
  "Create a finite seq of pseudorandom non-integer values."
  [n]
  (let [max-size 4]
    (take n (gen/sample-seq
              (gen/such-that (complement integer?) gen/any)
              max-size))))

(defn rand-range
  "Generate a random integer between lower (inclusive) and upper
  (exclusive)."
  [^java.util.Random r lower upper]
  (+ lower (.nextInt r (- upper lower))))

(defn rand-hash
  "Generate a random hexadecimal string of specified length."
  [r n]
  (let [c (count hexadecimal-chars)]
    (->> #(nth hexadecimal-chars (rand-range r 1 c))
      (repeatedly n)
      (apply str))))

(defn- rand-byte-buffer
  "Generate a ByteBuffer instance of specified length with random
  contents. The ByteBuffer instance is returned with its position set to
  zero."
  [^java.util.Random r n]
  (let [ba (byte-array n)]
    (.nextBytes r ba)
    (ByteBuffer/wrap ba)))

(defn generate-chunks-to-write
  "Create a finite seq of pseudorandom [hash ref-count bytes] argument
  tuples for the write-chunk method of the ChunkStore protocol."
  [^java.util.Random r n]
  (let [hash-fn #(rand-hash r (rand-range r 30 50))
        ref-count-fn #(rand-range r 0 10000)
        bytes-fn #(rand-byte-buffer r (rand-range r 1 40000))]
    (->> (juxt hash-fn ref-count-fn bytes-fn)
      (repeatedly)
      (take n))))

(defn test-write-chunk-single-threaded
  "Test ChunkStore properties with sequential calls to the write-chunk
  method in a single thread.

  chunks-to-write is a seq of

  [^String hash ^Integer ref-count ^java.nio.ByteBuffer bytes]

  tuples to write."
  [chunk-store chunks-to-write & {:keys [skip-refs?]}]
  (testing "write-chunk method mutates the chunk store"
    (doseq [[hash ref-count bytes] chunks-to-write]
      (is (nil? (proto/write-chunk chunk-store hash ref-count bytes))
          "write-chunk method returns nil")
      (when-not skip-refs?
        (is (= (proto/chunk-refs chunk-store hash) ref-count)
            "write-chunk sets the reference count"))
      (is (= (proto/read-chunk chunk-store hash) bytes)
          "write-chunk sets byte[] contents")))
  (testing "write-chunk method is idempotent"
    (doseq [[hash ref-count bytes] chunks-to-write]
      (let [_ (is (nil? (proto/write-chunk chunk-store hash ref-count bytes))
                  "write-chunk method returns nil")
            before [(proto/read-chunk chunk-store hash)
                    (proto/chunk-refs chunk-store hash)]
            _ (is (nil? (proto/write-chunk chunk-store hash ref-count bytes))
                  "write-chunk method returns nil")
            after [(proto/read-chunk chunk-store hash)
                   (proto/chunk-refs chunk-store hash)]]
        (is (and
              (= (first before) (first after))
              (= (second before) (second after)))
            "repeatedly write-chunk calls do not continue to mutate state"))))
  (testing "write-chunk method throws exception on invalid arguments"
    (let [[hash ref-count bytes] (first chunks-to-write)]
      (is (thrown?
            Exception
            (proto/write-chunk chunk-store "" ref-count bytes))
          "Calling write-chunk with empty string hash throws exception")
      (doseq [bytes* [(ByteBuffer/wrap (byte-array 0))
                      (ByteBuffer/wrap (byte-array 10) 10 0)
                      (.get (ByteBuffer/wrap (byte-array 5)) (byte-array 5))]]
        (is (thrown?
              Exception
              (proto/write-chunk chunk-store "" ref-count bytes*))
             "Calling write-chunk with zero remaining bytes throws exception"))
      (doseq [hash* (generate-non-strings 100)]
        (is (thrown?
              Exception
              (proto/write-chunk chunk-store hash* ref-count bytes))
            "Calling write-chunk with non-string hash throws exception"))
      (doseq [ref-count* (generate-non-integers 100)]
        (is (thrown?
              Exception
              (proto/write-chunk chunk-store hash ref-count* bytes))
            "Calling write-chunk with non-integer ref-count throws exception"))
      (doseq [bytes* [(.asCharBuffer bytes)
                      (.array bytes)
                      (seq (.array bytes))
                      (Hex/encodeHexString (.array bytes))]]
        (is (thrown?
              Exception
              (proto/write-chunk chunk-store hash ref-count bytes*))
            "Calling write-chunk with non-ByteBuffer bytes throws exception")))))

(defn test-update-chunk-refs-single-threaded
  "Test ChunkStore properties with sequential calls to the
  update-chunk-refs method in a single thread.

  chunk-refs-to-update is a seq of

      [^String hash ^Integer delta]

  tuples of ref-count updates to make.

  non-existent chunks is a seq of hashes (strings) that are expected to
  not be found in the store."
  [chunk-store chunk-refs-to-update nonexistent-chunks]
  (testing "update-chunk-refs method correctly updates chunk reference count"
    (doseq [[hash delta] chunk-refs-to-update]
      (let [before (proto/chunk-refs chunk-store hash)
            _ (is (nil? (proto/update-chunk-refs chunk-store hash delta))
                  "update-chunk-refs method returns nil")
            after (proto/chunk-refs chunk-store hash)]
        (is (= delta (- after before))
            "chunk reference count is incremented by the expected delta")))
    (let [hash (ffirst chunk-refs-to-update)
          before (proto/chunk-refs chunk-store hash)
          deltas (shuffle (map second chunk-refs-to-update))]
      (doseq [delta deltas]
        (proto/update-chunk-refs chunk-store hash delta))
      (is (= (apply + deltas)
             (- (proto/chunk-refs chunk-store hash) before))
          "multiple calls to update-chunk-refs accumulate their updates")))
  (testing "update-chunk-refs method throws exception on invalid arguments"
    (doseq [[hash* delta] (zip
                            nonexistent-chunks
                            (map second chunk-refs-to-update))]
      (is (thrown?
            Exception
            (proto/update-chunk-refs chunk-store hash* delta))
          "Calling update-chunks-refs on non-existent chunk throws exception"))
    (doseq [[hash* delta] (zip
                            (generate-non-strings (count chunk-refs-to-update))
                            (map second chunk-refs-to-update))]
      (is (thrown?
            Exception
            (proto/update-chunk-refs chunk-store hash* delta))
          "Calling update-chunk-refs with non-string hash throws exception"))
    (doseq [[hash delta*] (zip
                            (map first chunk-refs-to-update)
                            (generate-non-integers
                              (count chunk-refs-to-update)))]
      (is (thrown?
            Exception
            (proto/update-chunk-refs chunk-store hash delta*))
          "Calling update-chunk-refs with non-integer delta throws exception"))))

(defn test-read-chunk-single-threaded
  "Test ChunkStore properties with sequential calls to the read-chunk
  method in a single thread.

  written-chunks is a seq of hashes (strings) that are expected to be
  found in the store.

  non-existent chunks is a seq of hashes (strings) that are expected to
  not be found in the store."
  [chunk-store written-chunks nonexistent-chunks]
  (testing
    "read-chunk method has no write side effects and returns a ByteBuffer"
    (doseq [hash written-chunks]
      (let [results (repeatedly 3 #(proto/read-chunk chunk-store hash))]
        (is (every? #(instance? java.nio.ByteBuffer %) results)
            "Calling read-chunk returns a ByteBuffer")
        (is (apply = results)
            (str "Calling read-chunk multiple times without intervening "
                 "updates returns equal ByteBuffer instances")))))
  (testing "read-chunk method throws exception on invalid argument"
    (doseq [hash* nonexistent-chunks]
      (is (thrown? Exception (proto/read-chunk chunk-store hash*))
          "Calling read-chunk with nonexistent hash throws exception"))
    (doseq [hash* (generate-non-strings 100)]
      (is (thrown? Exception (proto/read-chunk chunk-store hash*))
          "Calling read-chunk with non-string hash throws exception"))
    (is (thrown? Exception (proto/read-chunk chunk-store ""))
        "Calling read-chunk with empty string hash throws exception")))

(defn test-chunk-refs-single-threaded
  "Test ChunkStore properties with sequential calls to the chunk-refs
  method in a single thread.

  written-chunks is a seq of hashes (strings) that are expected to be
  found in the store.

  non-existent chunks is a seq of hashes (strings) that are expected to
  not be found in the store."
  [chunk-store written-chunks nonexistent-chunks]
  (testing
    "chunk-refs method has no write side effects and returns an integer"
    (doseq [hash written-chunks]
      (let [results (repeatedly 3 #(proto/chunk-refs chunk-store hash))]
        (is (every? integer? results)
            "Calling read-chunk returns a ByteBuffer")
        (is (apply = results)
            (str "Calling read-chunk multiple times without intervening "
                 "updates returns the same integer")))))
  (testing "chunk-refs method throws exception on invalid argument"
    (doseq [hash* nonexistent-chunks]
      (is (thrown? Exception (proto/chunk-refs chunk-store hash*))
          "Calling chunk-refs with nonexistent hash throws exception"))
    (doseq [hash* (generate-non-strings 100)]
      (is (thrown? Exception (proto/chunk-refs chunk-store hash*))
          "Calling chunk-refs with non-string hash throws exception"))
    (is (thrown? Exception (proto/chunk-refs chunk-store ""))
        "Calling chunk-refs with empty string hash throws exception")))

(defn test-chunk-store-properties-single-threaded
  "Given a zero-argument setup function that returns a ChunkStore
  instance and a one-argument teardown function that cleans up this
  ChunkStore instance, calls chunk store methods sequentially in a
  single thread and check properties of a ChunkStore implementation.

  The caller is responsible for providing well-behaved setup and
  teardown arguments so that side effects are not persisted after this
  function returns.

  This function is intended for its side effects through the
  clojure.test/is assertion macro. Do not rely upon the return value.

  Usage example:

      (deftest my-chunk-store-implementation-single-threaded-test
        (let [setup (fn [] (->MyChunkStore))
              teardown (fn [chunk-store] (reset chunk-store))
              num-chunks 1000]
          (test-chunk-store-properties-single-threaded setup teardown num-chunks)))
  "
  [setup teardown num-chunks]
  (let [chunk-store (setup)
        r (java.util.Random.)]
    (try
      (testing
        (format "Single-threaded tests of ChunkStore %s:" (type chunk-store))
        (is (satisfies? proto/ChunkStore chunk-store)
            (format
              "%s instance implements the ChunkStore protocol"
              (type chunk-store)))
        (let [chunks-to-write (generate-chunks-to-write r num-chunks)
              written-chunks (map first chunks-to-write)
              nonexistent-chunks (rand-hash r num-chunks)
              chunk-refs-to-update (->> written-chunks
                                     (map (fn [hash]
                                            [hash (rand-range r -100 100)]))
                                     (shuffle))]
          (test-write-chunk-single-threaded
            chunk-store chunks-to-write)
          (test-read-chunk-single-threaded
            chunk-store written-chunks nonexistent-chunks)
          (test-chunk-refs-single-threaded
            chunk-store written-chunks nonexistent-chunks)
          (test-update-chunk-refs-single-threaded
            chunk-store chunk-refs-to-update nonexistent-chunks)))
      (finally (teardown chunk-store)))))

(defn test-write-chunk-multi-threaded
  [chunk-store chunks-to-write]
  (let [as (->> chunks-to-write ; (a b c ...)
             (map #(repeat 3 %)) ; ((a a a) (b b b) (c c c) ...)
             (apply concat) ; (a a a b b b c c c ...)
             (map agent))
        f (fn [[hash ref-count bytes]]
            (try
              (proto/write-chunk chunk-store hash ref-count bytes)
              (catch Exception e e)))
        results (do
                  ; Send action to agents
                  (doseq [a as] (send a f))
                  ; Wait for all actions to be completed
                  (apply await as)
                  ; Get results
                  (map deref as))]
    (is (every? nil? results)
        "concurrent write-chunk calls succeed")
    (doseq [[hash ref-count bytes] chunks-to-write]
      (is (= (proto/read-chunk chunk-store hash) bytes)
          "concurrent write-chunk calls are idempotent")
      (is (= (proto/chunk-refs chunk-store hash) ref-count)
          "concurrent write-chunk calls are idempotent"))))

(defn test-update-chunk-refs-multi-threaded
  [chunk-store ref-count-updates]
  (let [expected-deltas (->> ref-count-updates
                          (group-by first)
                          (map (fn [[hash updates]]
                                 [hash
                                  (apply + (map second updates))]))
                          (into {}))
        initial-ref-counts (->> ref-count-updates
                             (map first)
                             (distinct)
                             (map (fn [hash]
                                    [hash
                                     (proto/chunk-refs chunk-store hash)]))
                             (into {}))
        as (map agent ref-count-updates)
        f (fn [[hash delta]]
              (try
                (proto/update-chunk-refs chunk-store hash delta)
                (catch Exception e e)))
        results (do
                  ; Send action to agents
                  (doseq [a as] (send a f))
                  ; Wait for all actions to be completed
                  (apply await as)
                  ; Get results
                  (map deref as))]
    (is (every? nil? results)
        "concurrent update-chunk-refs calls succeed")
    (doseq [[hash expected-delta] expected-deltas]
      (is (= expected-delta
             (- (proto/chunk-refs chunk-store hash)
                (initial-ref-counts hash)))
          "concurrent update-chunk-refs calls are additive"))))

(defn test-chunk-store-properties-multi-threaded
  "Given a zero-argument setup function that returns a ChunkStore
  instance and a one-argument teardown function that cleans up this
  ChunkStore instance, calls chunk store methods concurrently in
  multiple threads and check properties of this ChunkStore
  implementation.

  The caller is responsible for providing well-behaved setup and
  teardown arguments so that side effects are not persisted after this
  function returns.

  This function is intended for its side effects through the
  clojure.test/is assertion macro. Do not rely upon the return value.

  Usage example:

      (deftest my-chunk-store-multi-threaded-test
        (let [setup (fn [] (apply ->MyChunkStore ctor-args))
              teardown (fn [chunk-store] (delete chunk-store))
              num-chunks 100]
          (test-chunk-store-properties-multi-threaded
            setup teardown num-chunks)))
  "
  [setup teardown num-chunks]
  (let [chunk-store (setup)
        r (java.util.Random.)
        chunks-to-write (generate-chunks-to-write r num-chunks)
        ref-count-updates (->> chunks-to-write
                            (map first)
                            (map #(repeat 3 %)) ; ((a a a) (b b b) (c c c) ...)
                            (apply concat) ; (a a a b b b c c c ...)
                            ; Assign random ref-count update to each element
                            (map (fn [hash]
                                   [hash (rand-range r -100 100)])))]
    (try
      (testing
        (format "Multi-threaded tests of ChunkStore %s:" (type chunk-store))
        (test-write-chunk-multi-threaded chunk-store chunks-to-write)
        (test-update-chunk-refs-multi-threaded chunk-store ref-count-updates))
      (finally (teardown chunk-store)))))
