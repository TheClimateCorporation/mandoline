(ns io.mandoline.slice-test
  (:use
   [midje sweet]
   [clojure test])
  (:require
   [io.mandoline.slice :as slice]))

(deftest mk-slice-test
  (facts "Test that slice/mk-slice works when given good input"
    (slice/mk-slice [0])  => (slice/->Slice [0] [1] [1])
    (slice/mk-slice [0] [2]) => (slice/->Slice [0] [2] [1])
    (slice/mk-slice [0] [3] [2]) => (slice/->Slice [0] [3] [2])
    (slice/mk-slice [0 0]) => (slice/->Slice [0 0] [1 1] [1 1])
    (slice/mk-slice [1 1] [2 2]) => (slice/->Slice [1 1] [2 2] [1 1])))

(deftest mk-slice-error-handling-test
  (facts "slice/mk-slice throws an error when given bad input"
    (slice/mk-slice [0 0] [1]) =>
    (throws java.lang.IllegalArgumentException
      "Start, stop and step do not all have the same parity ([0 0] [1] [1 1])")
    (slice/mk-slice [0 0] [2 2] [1 1 1]) =>
    (throws java.lang.IllegalArgumentException
      "Start, stop and step do not all have the same parity ([0 0] [2 2] [1 1 1])")))

(deftest get-shape-test
  (let [a (slice/->Slice [0 0] [4 4] [1 1])
        b (slice/->Slice [0 0] [4 4] [2 2])
        c (slice/->Slice [0 0] [5 4] [2 2])
        d (slice/->Slice [5 4] [0 0] [1 1])]
    (fact (slice/get-shape a) => [4 4])
    (fact (slice/get-shape b) => [2 2])
    (fact (slice/get-shape c) => [3 2])
    (fact (slice/get-shape d) => (throws java.lang.AssertionError))))

(deftest intersect-sorted-vector-test
  (facts "slice/intersect-sorted-vector"
    (slice/intersect-sorted-vector (range 100) (range 100000)) => (range 100)
    (slice/intersect-sorted-vector (range 100) (range 1 1000 3)) => (range 1 100 3)
    (slice/intersect-sorted-vector (range 2 6 3) (range 3 11 4)) => []))

(deftest get-intersection-test
  (facts "slice/get-intersection"
    (let [a (slice/->Slice [500 300 0] [550 330 20] [1 1 1])
          b (slice/->Slice [500 300 0] [550 330 20] [1 1 5])
          c (slice/->Slice [0 0 0] [1051 813 365] [1 1 1])
          d (slice/->Slice [0 0 0] [1051 813 365] [50 50 50])
          e (slice/->Slice [1 1 1] [5 5 5] [1 1 1])
          f (slice/->Slice [2 2 4] [8 4 6] [1 1 1])
          g (slice/->Slice [2 2] [8 4] [1 1])
          h (slice/->Slice [0 0 0] [1051 813 365] [25 25 11])
          i (slice/->Slice [7 3] [13 8] [1 1])
          j (slice/->Slice [2 4] [12 6] [2 1])
          k (slice/->Slice [8 4] [11 6] [2 1])
          l (slice/->Slice [1000 1000] [1200 1243] [1 1])
          m (slice/->Slice [0 0] [100 100] [1 1])
          n (slice/->Slice [] [] [])
          o (slice/->Slice [] [] [])]
      (future-fact "properly intersects striding slices (:step > 1)"
                   (slice/get-intersection a b) =>
                   (slice/->Slice [500 300 0] [550 330 16] [1 1 5])

                   (slice/get-intersection b c) =>
                   (slice/->Slice [500 300 0] [550 330 16] [1 1 5])

                   (slice/get-intersection b d) =>
                   (slice/->Slice [500 300 0] [501 301 1] [50 50 50])

                   (slice/get-intersection b h) =>
                   (slice/->Slice [500 300 0] [526 326 1] [25 25 55])

                   (slice/get-intersection i j) => k)

      (fact "slices must have a step of 1 in all dimensions"
            (slice/get-intersection a b) =>
            (throws java.lang.AssertionError))

      (fact "slices must have the same dimensionality"
            (slice/get-intersection e g) =>
            (throws java.lang.AssertionError))

      (slice/get-intersection a c) =>
      (slice/->Slice [500 300 0] [550 330 20] [1 1 1])

      (slice/get-intersection e f) =>
      (slice/->Slice [2 2 4] [5 4 5] [1 1 1])

      (slice/get-intersection l m) =>
      (throws java.lang.IllegalArgumentException
        #"The slices do not intersect: *")

      (slice/get-intersection n n) => n
      (slice/get-intersection n o) => n)))

(deftest contains-test
  (facts "slice/contains"
    (let [a (slice/->Slice [1 1 1] [11 11 11] [1 1 2])
          b (slice/->Slice [1 1 1] [11 11 11] [1 1 1])
          c (slice/->Slice [0 0 0] [11 11 11] [1 1 1])
          d (slice/->Slice [0 0 0] [12 12 12] [1 1 1])
          e (slice/->Slice [2 2 2] [10 10 10] [1 1 1])
          f (slice/->Slice [] [] [])]
      (slice/contains a b) => true
      (slice/contains a c) => true
      (slice/contains c d) => true
      (slice/contains a d) => true
      (slice/contains a e) => false
      (slice/contains a f) => false
      (slice/contains f a) => true
      (slice/contains e b d) => true)))

(deftest translate-test
  (facts "slice/translate"
    (let [a (slice/->Slice [1 1 1] [11 11 11] [1 1 2])
          b (slice/->Slice [1 1 1] [11 11 11] [1 1 1])
          c (slice/->Slice [0 0] [10 10] [1 1])
          d (slice/->Slice [0 0 0] [10 10 10] [1 1 1])
          e (slice/->Slice [2 3 4] [10 10 10] [1 1 1])
          f (slice/->Slice [3 4 5] [11 11 11] [1 1 1])
          g (slice/->Slice [2 3 4] [10 10 10] [2 2 2])
          h (slice/->Slice [3 4 5] [11 11 11] [2 2 2])
          i (slice/->Slice [0 0 0] [10 10 10] [1 1 2])]
      (slice/translate a b) => d
      (slice/translate b a) => i
      (slice/translate b c) => (throws java.lang.AssertionError)
      (slice/translate d b) => b
      (slice/translate b f) => e
      (slice/translate b h) => g)))

(deftest inter-test
  (facts "slice/iter"
    (let [a (slice/->Slice [0 0 0] [4 3 2] [1 1 1])]
      (slice/iter a) => `((0 0 0) (0 0 1) (0 1 0) (0 1 1) (0 2 0) (0 2 1)
                          (1 0 0) (1 0 1) (1 1 0) (1 1 1) (1 2 0) (1 2 1)
                          (2 0 0) (2 0 1) (2 1 0) (2 1 1) (2 2 0) (2 2 1)
                          (3 0 0) (3 0 1) (3 1 0) (3 1 1) (3 2 0) (3 2 1)))))
