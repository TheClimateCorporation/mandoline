(ns io.mandoline.chunk-test
  (:use
   [midje sweet]
   [clojure test])
  (:require
   [io.mandoline.chunk :as chunk]
   [io.mandoline.slice :as slice]
   [io.mandoline.slab :as slab])
  (:import ucar.ma2.Array))

(deftest to-chunk-coordinate-test
  (facts "to-chunk-coordinate returns expected output"
    (let [a (slice/->Slice [500 300 0] [550 330 20] [50 50 50])
          b (slice/->Slice [500 300 0] [551 330 20] [50 50 50])
          c (slice/->Slice [500 300 100] [520 310 120] [20 20 20])
          d (slice/->Slice [500 300 0] [520 310 120] [20 20 20])
          e (slice/->Slice [501 300 0] [520 310 120] [20 20 20])]
      (chunk/to-chunk-coordinate a a) => [[10 6 0]]
      (chunk/to-chunk-coordinate b b) => [[10 6 0] [11 6 0]]
      (chunk/to-chunk-coordinate c c) => [[25 15 5]]
      (chunk/to-chunk-coordinate c a) => [[10 6 2]]
      (chunk/to-chunk-coordinate d d) => [[25 15 0] [25 15 1] [25 15 2] [25 15 3] [25 15 4] [25 15 5]]
      (chunk/to-chunk-coordinate d a) => [[10 6 0] [10 6 1] [10 6 2]]
      (chunk/to-chunk-coordinate e e) => [[25 15 0] [25 15 1] [25 15 2] [25 15 3] [25 15 4] [25 15 5]])))

(deftest from-chunk-coordinate-test
  (facts "from-chunk-coordinate returns expected output"
    (let [a (slice/->Slice [0 0 0] [1051 813 20] [50 50 50])]
      (chunk/from-chunk-coordinate a [0 0 0]) =>
      (slice/->Slice [0 0 0] [50 50 50] [1 1 1])

      (chunk/from-chunk-coordinate a [0 1 2]) =>
      (slice/->Slice [0 50 100] [50 100 150] [1 1 1]))))

(deftest generate-id-test
  (facts "generate-id generates different ids"
    (let [a (->> (range 10) long-array (repeat 10) into-array Array/factory)
          b (->> (range 1 11) long-array (repeat 10) into-array Array/factory)
          a-slice (slice/mk-slice [0 0] [10 10])
          b-slice (slice/mk-slice [0 0] [10 10])
          chunk-a (slab/->Slab a a-slice)
          chunk-b (slab/->Slab b b-slice)]
      (string? (chunk/generate-id chunk-a)) => true
      (count (chunk/generate-id chunk-a)) => 40
      (= (chunk/generate-id chunk-a) (chunk/generate-id chunk-b)) => false)))

(future-fact "merge-overlap overwrites contents appropriately")

