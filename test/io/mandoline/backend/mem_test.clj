(ns io.mandoline.backend.mem-test
  (:use clojure.test
        clojure.test io.mandoline.backend.mem)
  (:require
    [clojure.test :refer :all]
    [io.mandoline.backend.mem :as mem]
    [io.mandoline.impl.protocol :as proto]
    [io.mandoline.test
     [concurrency :as concurrency]
     [entire-flow :as entire-flow]
     [failed-ingest :as failed-ingest]
     [grow :as grow]
     [invalid-metadata :as invalid-metadata]
     [linear-versions :as linear-versions]
     [nan :as nan]
     [overwrite :as overwrite]
     [scalar :as scalar]
     [shrink :as shrink]
     [utils :as utils]]
    [io.mandoline.test.protocol
     [schema :as schema]
     [chunk-store :as chunk-store]])
  (:import
    [java.nio ByteBuffer]
    [org.joda.time DateTime]))

(defn schema [] (mem/mk-schema :foo))

(def ^:private setup utils/setup-mem-spec)

(def ^:private teardown utils/teardown-mem-spec)

(deftest unit-test
  (is (identical? (.store (mem/mk-schema {:root :foo}))
                  (.store (mem/mk-schema {:root :foo}))))

  (let [sch (schema)
        name (utils/random-name)]
    (is (empty? (proto/list-datasets sch)))
    (proto/create-dataset sch name)
    (is (= [name] (proto/list-datasets sch)))
    (proto/destroy-dataset sch name))

  (let [sch (schema)
        name (utils/random-name)]
    (proto/create-dataset sch name)
    (proto/destroy-dataset sch name)
    (is (empty? (proto/list-datasets sch)))
    (proto/destroy-dataset sch name))

  (let [sch (schema)
        name (utils/random-name)
        _ (proto/create-dataset sch name)
        conn (proto/connect sch name)]
    (is (empty? (proto/versions conn {})))
    (proto/destroy-dataset sch name))

  (let [sch (schema)
        name (utils/random-name)
        _ (proto/create-dataset sch name)
        conn (proto/connect sch name)]
    (proto/write-version conn {:version-id 1234 :foo :bar})
    (is (= 1 (count (proto/versions conn {}))))
    (is (= "1234" (-> (proto/versions conn {}) first :version)))
    (is (= DateTime (-> (proto/versions conn {}) first :timestamp class)))
    (is (= {:foo :bar :version-id 1234} (proto/metadata conn "1234"))))

  (let [sch (schema)
        name (utils/random-name)
        _ (proto/create-dataset sch name)
        conn (proto/connect sch name)
        store (proto/chunk-store conn {})]
    (is (thrown? Exception (proto/read-chunk store "myhash")))
    (proto/write-chunk store "myhash" 5
                       (ByteBuffer/wrap (byte-array 5 (map byte (range 5)))))
    (is (thrown? Exception (proto/read-chunk store "badhash")))
    (is (= (range 5)
           (seq (.array (proto/read-chunk store "myhash")))))
    (is (= (range 5)
           (seq (.array (proto/read-chunk store "myhash")))))
    (proto/destroy-dataset sch name))

  (let [sch (schema)
        name (utils/random-name)
        _ (proto/create-dataset sch name)
        conn (proto/connect sch name)
        idx (proto/index conn :myvar {:version-id 1234} {})]
    (is (= {:metadata {:version-id 1234} :var-name :myvar}
           (proto/target idx)))
    (is (nil? (proto/chunk-at idx [0 1 2])))
    (proto/destroy-dataset sch name))

  (let [sch (schema)
        name (utils/random-name)
        _ (proto/create-dataset sch name)
        conn (proto/connect sch name)
        idx (proto/index conn :myvar {:version-id 1234} {})]

    (is (proto/write-index idx [0 2 3] nil "myhash1"))
    (is (proto/write-index idx [0 0 0] nil "myhash2"))
    (is (= "myhash1" (proto/chunk-at idx [0 2 3])))
    (is (= "myhash2" (proto/chunk-at idx [0 0 0])))

    (is (not (proto/write-index idx [0 0 0] nil "myhash3"))
        "inconsistent index write, the index was not new")
    (is (= "myhash2" (proto/chunk-at idx [0 0 0])))

    (is (proto/write-index idx [0 0 0] "myhash2" "myhash3")
        "inconsistent index write, the index old value is incorrect")
    (is (= "myhash3" (proto/chunk-at idx [0 0 0])))

    (proto/destroy-dataset sch name)))

(deftest test-mem-schema-properties
  (let [roots (atom {}) ; map of Schema instance -> root
        setup-schema (fn []
                       (let [store-spec (setup)
                             schema (mem/mk-schema store-spec)]
                         (swap! roots assoc schema (:root store-spec))
                         schema))
        teardown-schema (fn [schema]
                          (let [root (@roots schema)]
                            (mem/destroy-schema root)))
        num-datasets 10]
    (schema/test-schema-properties-single-threaded
      setup-schema teardown-schema num-datasets)
    (schema/test-schema-properties-multi-threaded
      setup-schema teardown-schema num-datasets)))

(deftest test-mem-chunk-store-properties
  (let [store (atom {:chunks {}})
        setup-chunk-store #(mem/->MemChunkStore store)
        teardown-chunk-store (fn [_] (reset! store nil))
        num-chunks 100]
    (chunk-store/test-chunk-store-properties-single-threaded
      setup-chunk-store teardown-chunk-store num-chunks)
    (chunk-store/test-chunk-store-properties-multi-threaded
      setup-chunk-store teardown-chunk-store num-chunks)))

(deftest mem-entire-flow
  (utils/with-and-without-caches
    (entire-flow/entire-flow setup teardown)))

(deftest mem-grow-dataset
  (utils/with-and-without-caches
    (grow/grow-dataset setup teardown)))

(deftest mem-shrink-dataset
  (utils/with-and-without-caches
    (shrink/shrink-dataset setup teardown)))

(deftest mem-overwrite-dataset
  (utils/with-and-without-caches
    (overwrite/overwrite-dataset setup teardown)))

(deftest mem-overwrite-extend-dataset
  (utils/with-and-without-caches
    (overwrite/overwrite-extend-dataset setup teardown)))

(deftest mem-invalid-metadata
  (utils/with-and-without-caches
    (invalid-metadata/invalid-metadata setup teardown)))

(deftest mem-change-metadata
  (utils/with-and-without-caches
    (invalid-metadata/change-metadata setup teardown)))

(deftest mem-lots-of-tiny-slices
  (utils/with-and-without-caches
    (concurrency/lots-of-tiny-slices setup teardown)))

(deftest mem-write-scalar
  (utils/with-and-without-caches
    (scalar/write-scalar setup teardown)))

(deftest mem-write-fail-write
  (utils/with-and-without-caches
    (failed-ingest/failed-write setup teardown)))

(deftest mem-linear-versions
  (utils/with-and-without-caches
    (linear-versions/linear-versions setup teardown)))

(deftest mem-nan-fill-values
  (utils/with-and-without-caches
    (nan/fill-double setup teardown)
    (nan/fill-float setup teardown)
    (nan/fill-short setup teardown)))
