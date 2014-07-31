(ns io.mandoline.filters.lz4-test
  (:use
   [clojure.test])
  (:require
    [io.mandoline.filters.lz4 :as lz4])
  (:import
    [java.nio ByteBuffer]))

(deftest test-lz4-filter
  (doseq [sample-size [64512 32768 512 0]]
    (let [sample-buffer (ByteBuffer/allocate sample-size)]
      (dotimes [i 1000]
        (.nextBytes (java.util.Random.) (.array sample-buffer))
        (let [compressed-buffer (lz4/filter-apply {} (.slice sample-buffer))
              uncompressed-buffer (lz4/filter-reverse compressed-buffer)]
          ;; never overflow 64k!
          (is (<= (.remaining compressed-buffer) (* 64 1024)))
          ;; maximum expansion is 1 byte!
          (is (<= (.remaining compressed-buffer) (inc sample-size)))
          ;; must reverse 1:1!
          (is (= sample-buffer uncompressed-buffer)))))))

(deftest test-lz4hc-filter
  (doseq [sample-size [64512 32768 512 0]]
    (let [sample-buffer (ByteBuffer/allocate sample-size)]
      (dotimes [i 1000]
        (.nextBytes (java.util.Random.) (.array sample-buffer))
        (let [compressed-buffer (lz4/filter-apply-hc {} (.slice sample-buffer))
              uncompressed-buffer (lz4/filter-reverse compressed-buffer)]
          ;; never overflow 64k!
          (is (<= (.remaining compressed-buffer) (* 64 1024)))
          ;; maximum expansion is 1 byte!
          (is (<= (.remaining compressed-buffer) (inc sample-size)))
          ;; must reverse 1:1!
          (is (= sample-buffer uncompressed-buffer)))))))
