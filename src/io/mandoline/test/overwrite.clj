(ns io.mandoline.test.overwrite
  (:use
   [clojure test])
  (:require
   [io.mandoline :as db]
   [io.mandoline.slab :as slab]
   [io.mandoline.slice :as slice]
   [io.mandoline.test.utils :as test-utils]))

(defn overwrite-dataset [setup teardown]
  (test-utils/with-temp-db store-spec setup teardown
    (let [foo-slice (slice/mk-slice [0 0 0] [10 11 12])
          foo (test-utils/same-slab "short" foo-slice 1)
          update-slice (slice/mk-slice [4 4 4] [5 5 5])
          update (test-utils/same-slab "short" update-slice 2)
          entire-foo (-> (test-utils/same-slab "short" foo-slice 1)
                         (slab/merge update))

          dds {:dimensions {:x 10 :y 11 :z 12}
               :variables {:foo {:type "short" :shape ["x" "y" "z"] :fill-value -3}}
               :chunk-dimensions {:x 2 :y 2 :z 2}}

          ds-writer (db/dataset-writer store-spec)

          ;;; writing version 1
          writer (-> ds-writer db/on-last-version (db/add-version dds))
          _ (with-open [foo-writer (-> writer (db/variable-writer :foo))]
              (db/write foo-writer [foo]))
          _ (db/finish-version writer)

          _ (Thread/sleep 2000) ;; so the versions get different timestamps

          ;;; writing version 2
          writer (-> ds-writer db/on-last-version (db/add-version (db/metadata store-spec)))
          _ (with-open [foo-writer (-> writer (db/variable-writer :foo))]
              (db/write foo-writer [update]))
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
        (is (db/get-slice v1-foo-reader foo-slice))
        (is ((test-utils/same-as foo)
             (db/get-slice v1-foo-reader foo-slice)))

        (is ((test-utils/same-as (test-utils/same-slab "short" update-slice 1))
             (db/get-slice v1-foo-reader update-slice))))

      (testing "data is returned as expected for the second version"
        (is (db/get-slice v2-foo-reader foo-slice))

        (is ((test-utils/same-as entire-foo)
             (db/get-slice v2-foo-reader foo-slice)))))))

(defn overwrite-extend-dataset [setup teardown]
  (test-utils/with-temp-db store-spec setup teardown
    (let [small-slice (slice/mk-slice [0 0 0] [10 11 12])
          small-foo (test-utils/same-slab "short" small-slice 1)
          more-slice (slice/mk-slice [5 5 5] [20 20 20])
          more-foo (test-utils/same-slab "short" more-slice 2)
          entire-slice (slice/mk-slice [0 0 0] [20 20 20])
          small-not-overlap (slice/mk-slice [0 0 0] [5 5 5])
          overlap-slice (slice/mk-slice [5 5 5] [10 11 12])
          entire-foo (-> (test-utils/same-slab "short" entire-slice -3)
                         (slab/merge small-foo)
                         (slab/merge more-foo))

          dds-v1 {:dimensions {:x 10, :y 11, :z 12}
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
                     (assoc :dimensions {:x 20 :y 20 :z 20}))
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

        (is ((test-utils/same-as (slab/subset small-foo overlap-slice))
             (db/get-slice v1-foo-reader overlap-slice)))

        (is (thrown? IndexOutOfBoundsException
                     (db/get-slice v1-foo-reader entire-slice))))

      (testing "data is returned as expected for the second version"
        (is (db/get-slice v2-foo-reader entire-slice))
        (is ((test-utils/same-as (slab/subset more-foo overlap-slice))
             (db/get-slice v2-foo-reader overlap-slice)))

        (is ((test-utils/same-as (slab/subset small-foo small-not-overlap))
             (db/get-slice v2-foo-reader small-not-overlap)))

        (is ((test-utils/same-as more-foo)
             (db/get-slice v2-foo-reader more-slice)))

        (is ((test-utils/same-as entire-foo)
             (db/get-slice v2-foo-reader entire-slice)))))))
