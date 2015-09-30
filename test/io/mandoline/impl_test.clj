(ns io.mandoline.impl-test
  (:require
    [clojure.test :refer :all]
    [io.mandoline
     [chunk :as chunk]
     [impl :as impl]
     [slab :as slab]
     [slice :as slice]]
    [io.mandoline.impl.protocol :as proto])
  (:import
    [java.nio ByteBuffer]
    [ucar.ma2 Array DataType]))


;; Verify that when writing a variable, we don't repeatedly write an identical
;; chunk. That's wasteful.
(deftest write-variable-dedups-chunks
  (let [chunks-written (atom 0)
        float-type (slab/as-data-type Float/TYPE)
        zeros-chunk (slab/empty float-type (slice/mk-slice [0 0] [10 10])) 
        full-data (slab/empty
                    float-type (slice/mk-slice [0 0] [1000 1000]) 1.0)]
    (with-redefs [impl/get-base-chunk (constantly zeros-chunk)
                  proto/write-chunk (fn [& _] (swap! chunks-written inc))
                  proto/target (constantly
                                 {:metadata
                                  {:version-id :version-0
                                   :variables {:var {:shape [:dim1 :dim2]}}
                                   :dimensions {:dim1 1000
                                                :dim2 1000}
                                   :chunk-dimensions {:dim1 10
                                                      :dim2 10}}
                                  :var-name :var})
                  proto/write-index (constantly true)
                  impl/chunk-at (constantly (chunk/generate-id zeros-chunk))]
      (impl/write-variable :store :index nil [full-data])
      ;; Even though we were writing thousands of chunks, they were all the same,
      ;; and hopefully we didn't write that one chunk too many times.
      (is (< 0 @chunks-written 100)))))

(deftest test-bytes-to-array
  (let [elements (map byte (range 30))
        ; Create a ByteBuffer instance that is backed by a 25-element
        ; byte array, with position=3 and limit=19. Diagram:
        ;
        ; byte[25] =========================
        ; slice        =====================
        ; limit        ===================
        ; position        ================
        byte-buffer (-> (byte-array elements)
                      (ByteBuffer/wrap)
                      (.position 4) ; advance position by 4
                      (.slice) ; create a slice that shares the underlying byte[]
                      (.limit 19) ; trim to 19 long
                      (.position 3)) ; advance position by 3
        data-type DataType/BYTE
        shape [2 4 2]
        array (impl/bytes-to-array byte-buffer data-type shape)]
    (is (instance? Array array)
        "bytes-to-array returns an instance of ucar.ma2.Array")
    (is (= (.getClassType data-type) (.getElementType array))
        "bytes-to-array returns an Array with the expected element type")
    (is (= shape (vec (.getShape array)))
        "bytes-to-array returns an Array with the expected shape")
    (is (= (take (- 19 3) (drop (+ 4 3) elements)) (seq (.copyTo1DJavaArray array)))
        "bytes-to-array returns an Array that contains the expected elements")
    (is (= 3 (.position byte-buffer))
        "bytes-to-array does not change the position of the original ByteBuffer")
    (is (= 19 (.limit byte-buffer))
        "bytes-to-array does not change the limit of the original ByteBuffer")
    (is (thrown? IllegalArgumentException (impl/bytes-to-array nil data-type shape))
        "bytes-to-array throws an IllegalArgumentException when passed null reference")))
