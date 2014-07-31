(ns io.mandoline.filters.chain-test
  (:use
    [clojure.test]
    [io.mandoline.filters.chain])
  (:import
    [java.nio ByteBuffer]))

(def test-chains
  [[[] [0 0]]
   [["lz4"] [0 1 1]]
   [["deflate"] [0 1 3]]
   [["lz4hc"] [0 1 2]]
   [["lz4" "lz4hc"] [0 2 1 2]]
   [["deflate" "lz4"] [0 2 3 1]]
   [["lz4" "lz4" "lz4hc"] [0 3 1 1 2]]])

(def test-filters ["lz4" "lz4hc" "deflate"])

(deftest test-chain-headers
  (let [sample-buffer (ByteBuffer/allocate (* 32 1024))]
    (doseq [[filters expected-header] test-chains]
      (.nextBytes (java.util.Random.) (.array sample-buffer))
      (let [chain-apply (get-chain-apply {:filters filters} :na)
            chunk-header (byte-array (count expected-header))
            filtered-buffer (chain-apply (.slice sample-buffer))]
        (.get filtered-buffer chunk-header)
        (is (= (vec chunk-header) expected-header))))))

(deftest test-chain-roundtrip
  (doseq [sample-size [64512 32768 512 0]]
    (let [sample-buffer (ByteBuffer/allocate sample-size)]
      (dotimes [iteration 1000]
        (.nextBytes (java.util.Random.) (.array sample-buffer))
        (let [filters (repeatedly (rand-int 10) #(rand-nth test-filters))
              chain-apply (get-chain-apply {:filters filters} :na)
              chain-reverse (get-chain-reverse)
              filtered-buffer (chain-apply (.slice sample-buffer))
              unfiltered-buffer (chain-reverse filtered-buffer)]
          (is (= unfiltered-buffer sample-buffer)))))))
