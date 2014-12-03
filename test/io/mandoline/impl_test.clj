(ns io.mandoline.impl-test (:require
    [clojure.test :refer :all]
    [io.mandoline
     [chunk :as chunk]
     [impl :as impl]
     [slab :as slab]
     [slice :as slice]]
    [io.mandoline.impl.protocol :as proto]))


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
