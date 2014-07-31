(ns io.mandoline.slab-test
  (:use
   [midje sweet]
   [clojure test])
  (:require
   [io.mandoline.slab :as slab]
   [io.mandoline.slice :as slice]
   [io.mandoline.test.utils :as test-utils])
  (:import
   [ucar.ma2 Array DataType]))

(deftest empty-test
  (facts "empty returns arrays of the right shape and fill."
    (let [a-slice (slice/->Slice [1 1 1] [6 7 8] [1 1 1])
          a (slab/empty DataType/INT a-slice)
          b-slice (slice/->Slice [0 0] [100 2] [1 1 1])
          b (slab/empty DataType/SHORT b-slice -1)]
      (test-utils/get-shape a) => [5 6 7]
      (test-utils/get-underlying-array a) => (repeat 210 0)
      (test-utils/get-data-type a) => DataType/INT
      (test-utils/get-shape b) => [100 2]
      (test-utils/get-underlying-array b) => (repeat 200 -1)
      (test-utils/get-data-type b) => DataType/SHORT)))

(deftest subset-test
  (facts "subset properly returns a section of the slab"
    (let [x-slice (slice/mk-slice [7 3] [13 8])
          x (test-utils/to-slab "short" x-slice
                          '(1 2 3 4 5 2 3 4 5 6 3 4 5 6 7 4 5 6 7 8 5 6 7 8 9 6 7 8 9 0))

          u-slice (slice/->Slice [7 3] [10 5] [1 1])
          u (test-utils/to-slab "short" u-slice '(1 2 2 3 3 4))

          v-slice (slice/->Slice [8 4] [12 6] [2 1])
          v (test-utils/to-slab "short" v-slice '(3 4 5 6))

          y-slice (slice/->Slice [0 0] [2 3] [1 1])

          z-slice (slice/mk-slice [0 0] [12 10] [2 2])
          z (test-utils/to-slab "short" z-slice
                          '(1 2 3 4 5 2 3 4 5 6 3 4 5 6 7 4 5 6 7 8 5 6 7 8 9 6 7 8 9 0))

          w-slice (slice/->Slice [7 3] [9 7] [2 2])
          w (test-utils/to-slab "short" w-slice '(1 3))]
      ;; proper subsetting
      (slab/subset x u-slice) => (test-utils/same-as u)
      (slab/subset x v-slice) => (test-utils/same-as v)
      (slab/subset x w-slice) => (test-utils/same-as w)
      ;; throws assertion error when requested slice does not overlap
      (slab/subset x y-slice) => (throws java.lang.AssertionError)
      ;; throws assertion error when step sizes are different
      (slab/subset z v-slice) => (throws java.lang.AssertionError))))

(deftest intersect-test
  (facts "intersect properly returns a section of the slab"
    (let [x-slice (slice/mk-slice [7 3] [13 8])
          x (test-utils/to-slab "short" x-slice
                          '(1 2 3 4 5 2 3 4 5 6 3 4 5 6 7 4 5 6 7 8 5 6 7 8 9 6 7 8 9 0))

          u-slice (slice/->Slice [7 3] [10 5] [1 1])
          u (test-utils/to-slab "short" u-slice '(1 2 2 3 3 4))

          v-slice (slice/->Slice [8 4] [12 6] [2 1])
          v (test-utils/to-slab "short" v-slice '(3 4 5 6))

          y-slice (slice/->Slice [0 0] [2 3] [1 1])

          w-slice (slice/->Slice [7 3] [9 7] [2 2])
          w (test-utils/to-slab "short" w-slice '(1 3))

          z-slice (slice/->Slice [0 0 0] [1 2 3] [1 1 1])
          z (test-utils/to-slab "short" z-slice '(1 1 2 2 3 3))]
      ;; proper intersections
      (slab/intersect x (slice/mk-slice [0 0] [10 5])) =>
      (test-utils/same-as u)

      (fact "slabs must have the same dimensionality"
            (slab/intersect z y-slice) =>
            (throws java.lang.AssertionError))

      (fact "slabs must have a step of 1 in all dimensions"
            (slab/intersect w v-slice) =>
            (throws java.lang.AssertionError))

      (future-fact "striding slices"
                   (slab/intersect x (slice/->Slice [4 4] [12 6] [2 1])) =>
                   (test-utils/same-as v)

                   (slab/intersect x (slice/->Slice [1 1] [9 7] [2 2])) =>
                   (test-utils/same-as w)))))

(deftest copy-into-test
  (let [x-slice (slice/mk-slice [7 3] [13 8])
        x (test-utils/to-slab "short" x-slice
                        '(1 2 3 4 5 2 3 4 5 6 3 4 5 6 7 4 5 6 7 8 5 6 7 8 9 6 7 8 9 0))

        u (test-utils/same-slab "short" (slice/mk-slice [7 3] [10 5]) -1)
        uu (test-utils/to-slab "short" x-slice
                         '(-1 -1 3 4 5 -1 -1 4 5 6 -1 -1 5 6 7 4 5 6 7 8 5 6 7 8 9 6 7 8 9 0))

        v-slice (slice/->Slice [8 4] [12 6] [2 1])
        v (test-utils/same-slab "short" v-slice -2)

        vv (test-utils/to-slab "short" x-slice
                         '(1 2 3 4 5 2 -2 -2 5 6 3 4 5 6 7 4 -2 -2 7 8 5 6 7 8 9 6 7 8 9 0))

        y (test-utils/same-slab "int" v-slice -2)
        z (test-utils/same-slab "short" (slice/->Slice [0 0] [2 2] [1 1]) -1)]
    (facts "copy-into properly returns a section of the slab"
      (with-state-changes
        [(around :facts
           (let [x (test-utils/to-slab "short" x-slice
                    '(1 2 3 4 5 2 3 4 5 6 3 4 5 6 7 4 5 6 7 8 5 6 7 8 9 6 7 8 9 0))]
             ?form))]

        (fact (slab/copy-into u x) => (test-utils/same-as uu))
        (fact (slab/copy-into v x) => (test-utils/same-as vv))
        (fact (slab/copy-into y x) =>
          (throws java.lang.AssertionError
            "Assert failed: (= (get-data-type src) (get-data-type dst))"))
        (fact (slab/copy-into z x) =>
          (throws java.lang.AssertionError
            "Assert failed: (slice/contains (:slice src) (:slice dst))"))))))

(deftest merge-test
  (facts "merge properly returns a merged slab"
    (let [x (test-utils/to-slab "short" (slice/mk-slice [0 0] [3 5])
                          '(1 2 3 4 5 2 3 4 5 6 3 4 5 6 7))
          y (test-utils/to-slab "short" (slice/mk-slice [3 0] [7 5])
                          '(4 5 6 7 8 5 6 7 8 9 6 7 8 9 0 1 1 1 1 1))

          z-slice (slice/mk-slice [0 0] [6 5])
          z (test-utils/to-slab "short" z-slice
             '(1 2 3 4 5 2 3 4 5 6 3 4 5 6 7 4 5 6 7 8 5 6 7 8 9 6 7 8 9 0))


          xx (test-utils/to-slab "short" z-slice
              '(1 2 3 4 5 2 3 4 5 6 3 4 5 6 7 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1))
          yy (test-utils/to-slab "short" z-slice
              '(-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 4 5 6 7 8 5 6 7 8 9 6 7 8 9 0))]
      (with-state-changes
        [(around :facts (let [dst (slab/empty DataType/SHORT z-slice -1)] ?form))]
        (fact (slab/merge dst x) => (test-utils/same-as xx))
        (fact (slab/merge dst y) => (test-utils/same-as yy))
        (fact (reduce slab/merge dst [x y]) => (test-utils/same-as z))))))
