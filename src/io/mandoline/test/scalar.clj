(ns io.mandoline.test.scalar
  (:use
   [clojure test])
  (:require
   [io.mandoline :as db]
   [io.mandoline.slice :as slice]
   [io.mandoline.test.utils :as test-utils]))

(defn write-scalar [setup teardown]
  (test-utils/with-temp-db store-spec setup teardown
    (let [zero-slice (slice/mk-slice [] [])
          foo (test-utils/same-slab "short" zero-slice 1)
          bar (test-utils/same-slab "byte" zero-slice 0)

          dds {:dimensions {:x 10}
               :variables {:foo {:type "short" :shape [] :fill-value -999}
                           :bar {:type "byte" :shape [] :fill-value -1}}
               :chunk-dimensions {:x 2}}

          ds-writer (db/dataset-writer store-spec)

          ;;; writing scalar
          writer (-> ds-writer db/on-last-version (db/add-version dds))
          _ (with-open [foo-writer (-> writer (db/variable-writer :foo))]
              (db/write foo-writer [foo]))
          _ (with-open [bar-writer (-> writer (db/variable-writer :bar))]
              (db/write bar-writer [bar]))
          _ (db/finish-version writer)

          ;;; reading stuff
          version (-> (db/versions store-spec) first second)
          token (db/metadata store-spec)
          foo-reader (-> store-spec db/dataset-reader
                         (db/on-version version)
                         (db/variable-reader :foo))
          bar-reader (-> store-spec db/dataset-reader
                         (db/on-version version)
                         (db/variable-reader :bar))]

      (testing "writing a variable with no shape"
        (is (db/get-slice foo-reader zero-slice))
        (is ((test-utils/same-as foo)
             (db/get-slice foo-reader zero-slice)))

        (is (db/get-slice bar-reader zero-slice))
        (is ((test-utils/same-as bar)
             (db/get-slice bar-reader zero-slice)))

        (is (thrown? IndexOutOfBoundsException
                     (db/get-slice foo-reader (slice/mk-slice [0] [1]))))))))
