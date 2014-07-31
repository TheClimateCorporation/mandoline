(ns io.mandoline.test.shrink
  (:use
   [clojure test])
  (:require
   [io.mandoline :as db]
   [io.mandoline.slab :as slab]
   [io.mandoline.slice :as slice]
   [io.mandoline.test.utils :as test-utils]))

(defn shrink-dataset [setup teardown]
  (test-utils/with-temp-db store-spec setup teardown
    (let [big-slice (slice/mk-slice [0 0 0] [20 20 20])
          big-foo (test-utils/same-slab "short" big-slice 2)
          small-slice (slice/mk-slice [0 0 0] [10 11 12])
          small-foo (test-utils/same-slab "short" small-slice 1)

          dds-v1 {:dimensions {:x 20, :y 20, :z 20}
                  :variables {:foo {:type "short" :shape ["x" "y" "z"] :fill-value -3}}
                  :chunk-dimensions {:x 2 :y 2 :z 2}}

          ds-writer (db/dataset-writer store-spec)

          ;;; writing version 1
          writer (-> ds-writer db/on-last-version (db/add-version dds-v1))
          _ (with-open [foo-writer (-> writer (db/variable-writer :foo))]
              (db/write foo-writer [big-foo]))
          _ (db/finish-version writer)

          _ (Thread/sleep 2000) ;; so the versions get different timestamps

          ;;; writing version 2
          dds-v2 (-> (db/metadata store-spec)
                     (assoc :dimensions {:x 10 :y 11 :z 12}))
          writer (-> ds-writer db/on-last-version (db/add-version dds-v2))
          _ (with-open [foo-writer (-> writer (db/variable-writer :foo))]
              (db/write foo-writer [small-foo]))
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
      (testing "data is different between big-foo and small-foo"
        (is (not ((test-utils/same-as small-foo)
                  (slab/subset big-foo small-slice)))))

      (testing "data is returned properly for the first version"
        (is (db/get-slice v1-foo-reader small-slice))
        (is ((test-utils/same-as (slab/subset big-foo small-slice))
             (db/get-slice v1-foo-reader small-slice)))

        (is (db/get-slice v1-foo-reader big-slice))
        (is ((test-utils/same-as big-foo)
             (db/get-slice v1-foo-reader big-slice))))

      (testing "data is returned as expected for the second version"
        (is (db/get-slice v2-foo-reader small-slice))
        (is ((test-utils/same-as small-foo)
             (db/get-slice v2-foo-reader small-slice)))

        (is (thrown? IndexOutOfBoundsException
                     (db/get-slice v2-foo-reader big-slice)))))))
