(ns io.mandoline.test.grow
  (:use
   [clojure test])
  (:require
   [io.mandoline :as db]
   [io.mandoline.slab :as slab]
   [io.mandoline.slice :as slice]
   [io.mandoline.test.utils :as test-utils]))

(defn grow-dataset [setup teardown]
  (test-utils/with-temp-db store-spec setup teardown
    (let [small-slice (slice/mk-slice [0 0 0] [4 5 6])
          small-foo (test-utils/same-slab "short" small-slice 1)
          more-slice (slice/mk-slice [4 5 6] [10 10 10])
          more-foo (test-utils/same-slab "short" more-slice 2)
          entire-slice (slice/mk-slice [0 0 0] [10 10 10])
          entire-foo (-> (test-utils/same-slab "short" entire-slice -3)
                         (slab/merge more-foo)
                         (slab/merge small-foo))

          dds-v1 {:dimensions {:x 4 :y 5 :z 6}
                  :variables {:foo {:type "short" :shape ["x" "y" "z"] :fill-value -3}}
                  :chunk-dimensions {:x 2 :y 2 :z 2}}

          ds-writer (db/dataset-writer store-spec)

          ;;; writing version 1
          writer (-> ds-writer db/on-last-version (db/add-version dds-v1))
          _ (with-open [foo-writer (-> writer (db/variable-writer :foo))]
              (db/write foo-writer [small-foo]))
          _ (db/finish-version writer)

          _ (Thread/sleep 2000) ;; so the versions get different timestamps

          ;;; writing version 2
          dds-v2 (-> (db/metadata store-spec)
                     (assoc :dimensions {:x 10 :y 10 :z 10}))
          writer (-> ds-writer db/on-last-version (db/add-version dds-v2))
          _ (with-open [foo-writer (-> writer (db/variable-writer :foo))]
              (db/write foo-writer [more-foo]))
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
        (is (db/get-slice v1-foo-reader small-slice))
        (is ((test-utils/same-as small-foo)
             (db/get-slice v1-foo-reader small-slice)))

        (is (thrown? IndexOutOfBoundsException
                     (db/get-slice v1-foo-reader entire-slice)))

        (is (thrown? IndexOutOfBoundsException
                     (db/get-slice v1-foo-reader
                                   (slice/mk-slice [0 0 0] [4 6 6])))))

      (testing "data is returned as expected for the second version"
        (is (db/get-slice v2-foo-reader entire-slice))

        (is ((test-utils/same-as entire-foo)
             (db/get-slice v2-foo-reader entire-slice)))))))
