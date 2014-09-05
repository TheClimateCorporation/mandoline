(ns io.mandoline.test.concurrency
  (:use
   [clojure test])
  (:require
   [clojure.java.io :as jio]
   [clojure.tools.logging :as log]
   [io.mandoline :as db]
   [io.mandoline.slab :as slab]
   [io.mandoline.slice :as slice]
   [io.mandoline.utils :as utils]
   [io.mandoline.test.utils :as test-utils]))

(defn lots-of-tiny-slices [setup teardown]
  (test-utils/with-temp-db store-spec setup teardown
    (let [v1-slice (slice/mk-slice [0 0] [50 100])
          v2-slice (slice/mk-slice [0 0] [50 110])
          expected-foo-1 (test-utils/same-slab "short" v1-slice 1)
          expected-foo-2 (->> (test-utils/same-slab "short" (slice/mk-slice [0 85] [50 110]) 2)
                              (slab/merge (test-utils/same-slab "short" v2-slice 1)))

          foos (for [offset (range 0 100)]
                 (test-utils/same-slab "short" (slice/mk-slice [0 offset] [50 (inc offset)]) 1))

          more-foos (for [offset (range 85 110)]
                      (test-utils/same-slab "short" (slice/mk-slice [0 offset] [50 (inc offset)]) 2))

          dds-v1 {:dimensions {:x 50 :y 100}
                  :variables {:foo {:type "short" :shape ["x" "y"] :fill-value -3}}
                  :chunk-dimensions {:x 5 :y 5}}

          ds-writer (db/dataset-writer store-spec)

          ;;; writing version 1
          writer (-> ds-writer db/on-last-version (db/add-version dds-v1))
          _ (with-open [foo-writer (-> writer (db/variable-writer :foo))]
              (db/write foo-writer (apply vector foos)))
          _ (db/finish-version writer)

          _ (Thread/sleep 2000) ;; so the versions get different timestamps

          ;;; writing version 2
          dds-v2 (-> (db/metadata store-spec)
                     (assoc :dimensions {:x 50 :y 110}))
          writer (-> ds-writer db/on-last-version (db/add-version dds-v2))
          _ (with-open [foo-writer (-> writer (db/variable-writer :foo))]
              (db/write foo-writer (apply vector more-foos)))
          _ (db/finish-version writer)

          ;;; reading stuff
          versions (db/versions store-spec)
          first-version (-> versions last second)
          second-version (-> versions first second)
          first-time (-> versions ffirst (.plusMillis -1))

          v1-foo-reader (-> store-spec db/dataset-reader
                            (db/on-timestamp first-time)
                            (db/variable-reader :foo))
          v2-foo-reader (-> store-spec db/dataset-reader
                            (db/on-last-version)
                            (db/variable-reader :foo))]

      (testing "data is returned properly for the first version"
        (is (db/get-slice v1-foo-reader v1-slice))
        (is ((test-utils/same-as expected-foo-1)
             (db/get-slice v1-foo-reader v1-slice)))

        (is (thrown? IndexOutOfBoundsException
                     (db/get-slice v1-foo-reader v2-slice))))

      (testing "data is returned as expected for the second version"
        (is (db/get-slice v2-foo-reader v2-slice))
        (is ((test-utils/same-as expected-foo-2)
             (db/get-slice v2-foo-reader v2-slice)))))))

(defn lots-of-overlaps
  "This tests that Mandoline properly merges multiple writes that
  overlap with a single chunk, thusly:

    +----------+----------+----------+
    | 0 0 0 0 0| 0 0 0 0 0| 0 0 0 0 0|
    | 0 0 0 0 0| 0 0 0 0 0| 0 0 0 0 0|
    | 0 0 0 0 0| 0 0 0 0 0| 0 0 0 0 0|
    | 0 0 0 1 1| 1 2 2 2 3| 3 3 0 0 0|
    | 0 0 0 1 1| 1 2 2 2 3| 3 3 0 0 0|
    +----------+----------+----------+
    | 0 0 0 1 1| 1 2 2 2 3| 3 3 0 0 0|
    | 0 0 0 4 4| 4 5 5 5 6| 6 6 0 0 0|
    | 0 0 0 4 4| 4 5 5 5 6| 6 6 0 0 0|
    | 0 0 0 4 4| 4 5 5 5 6| 6 6 0 0 0|
    | 0 0 0 7 7| 7 8 8 8 9| 9 9 0 0 0|
    +----------+----------+----------+
    | 0 0 0 7 7| 7 8 8 8 9| 9 9 0 0 0|
    | 0 0 0 7 7| 7 8 8 8 9| 9 9 0 0 0|
    | 0 0 0 0 0| 0 0 0 0 0| 0 0 0 0 0|
    | 0 0 0 0 0| 0 0 0 0 0| 0 0 0 0 0|
    | 0 0 0 0 0| 0 0 0 0 0| 0 0 0 0 0|
    +----------+----------+----------+

  In this test, nine separate writes intersect the middle chunk (from
  [5 5] to [10 10]). We apply each of these writes 1000 times, in
  random order, in an attempt to exercise the optimistic locking and
  retry behavior in impl/update-chunk!."
  [setup teardown]
  (test-utils/with-temp-db store-spec setup teardown
    (let [dds {:dimensions {:x 50 :y 50}
               :variables {:foo {:type "short"
                                 :shape ["x" "y"]
                                 :fill-value 0}}
               :chunk-dimensions {:x 7 :y 7}}

          mk-test-slab (fn [offset fill]
                         (let [s (->> (map #(+ 3 %) offset)
                                      (apply vector)
                                      (slice/mk-slice offset))]
                           (test-utils/same-slab "short" s fill)))

          ones   (mk-test-slab [3 9] 1)
          twos   (mk-test-slab [6 9] 2)
          threes (mk-test-slab [9 9] 3)
          fours  (mk-test-slab [3 6] 4)
          fives  (mk-test-slab [6 6] 5)
          sixes  (mk-test-slab [9 6] 6)
          sevens (mk-test-slab [3 3] 7)
          eights (mk-test-slab [6 3] 8)
          nines  (mk-test-slab [9 3] 9)
          slabs (->>
                 [ones twos threes fours fives sixes sevens eights nines]
                 (repeat 100)
                 (flatten)
                 (shuffle))

          base-slice (slice/mk-slice [0 0] [15 15])
          base-slab (test-utils/same-slab "short" base-slice 0)
          expected-foo (reduce slab/merge base-slab slabs)

          ds-writer (db/dataset-writer store-spec)

          ;;; writing stuff
          writer (-> ds-writer db/on-last-version (db/add-version dds))
          _ (->> slabs
                 (partition 10)
                 (utils/npmap
                  #(with-open [f (db/variable-writer writer
                                                     :foo
                                                     {:wrappers []})]
                     (db/write f (apply vector %))))
                 (dorun))
          _ (db/finish-version writer)

          ;;; reading stuff
          _ (Thread/sleep 1000)
          foo-reader (-> store-spec
                         (db/dataset-reader)
                         (db/on-last-version)
                         (db/variable-reader :foo))]

      (testing "data is merged properly"
        (is (db/get-slice foo-reader base-slice))
        (is ((test-utils/same-as expected-foo)
             (db/get-slice foo-reader base-slice)))))))

(defn lots-of-processes
  "This tests that Mandoline can handle distributed writes by instantiating
  subprocesses to run a script.  The script creates a variable writer, to
  write arbitrary data to arbitrary coordinates.  The script takes in the
  following arguments to customize the variable writing:

  fill, coordinates, variable name, store-spec, and the new metadata for the
  dataset.

  fill value: the intended constant for which the data will be written to, and
    not the fill value for the dataset.
  coordinates: the last dimension's coordinates, so that we can write
    data along one dimension.

  The rest of the inputs are fairly straight forward.

  This test is not run for the filesystem or memory backend stores
  because it doesn't make sense to need distributed writes for them."
  [setup teardown mis-ordered?]
  (test-utils/with-temp-db store-spec setup teardown
    (let [nprocesses 4
          entire-slice (slice/mk-slice [0 0] [50 100])
          expected-foo (test-utils/same-slab "short" entire-slice 1)
          coords (->> (if mis-ordered?
                        (shuffle (range 0 100))
                        (range 0 100))
                      (partition (/ 100 nprocesses)))

          dds {:dimensions {:x 50 :y 100}
               :variables {:foo {:type "short" :shape ["x" "y"] :fill-value -3}}
               :chunk-dimensions {:x 7 :y 7}}
          writer (-> store-spec
                     db/dataset-writer
                     db/on-last-version
                     (db/add-version dds))
          token (db/dataset-writer->token writer)
          proc (fn [& commands] (.start (ProcessBuilder. commands)))
          process-fn (fn [c]
                       (proc "lein2"
                             ;; we need to make sure our logger doesn't talk to
                             ;; the output stream, because we are using it for
                             ;; IPC, but we still want logging
                             "update-in" ":jvm-opts" "conj"
                             "\"-Droot.logger=INFO, file\"" "--"
                             "run" "-m" "io.mandoline.test.script"
                             "--fill" (str 1)
                             "--coordinates" (->> c (interpose ",") (apply str))
                             "--variable-name" "foo"
                             "--store-spec" (utils/generate-metadata store-spec)
                             "--token" (utils/generate-metadata token)))
          get-reader (fn [p] (-> (.getInputStream p) (jio/reader) line-seq))
          get-writer (fn [p] (-> (.getOutputStream p) jio/writer))

          _ (log/info "Starting processes")
          processes (doall (map process-fn coords))
          _ (log/infof "Processes started %s" (pr-str processes))
          readers (map get-reader processes)
          writers (map get-writer processes)
          ready? (every? #(= "r" %) (map first readers))
          _ (log/infof "Got ready from slaves")
          _ (log/infof "Sending 'go' instruction to slaves")
          _ (if ready? (doall (map #(spit % "go") writers)))
          _ (log/infof "'go' sent")
          responses (doall (map last readers))
          done? (every? #(= "done" %) responses)
          _ (log/infof "got 'done' from slaves")
          _ (if done? (do (db/finish-version writer)))

          _ (Thread/sleep 10000)
          ;; reading stuff
          foo-reader (-> store-spec db/dataset-reader
                         (db/on-last-version)
                         (db/variable-reader :foo))]

      (testing "Conditions are well"
        (is ready?)
        (is done?))

      (testing "Writing data with var writers in multiple subprocesses"
        (is (db/get-slice foo-reader entire-slice))
        (is ((test-utils/same-as expected-foo)
             (db/get-slice foo-reader entire-slice)))))))
