(ns io.mandoline.test.linear-versions
  (:use
   [clojure test])
  (:require
   [io.mandoline :as db]
   [io.mandoline.slice :as slice]
   [io.mandoline.test.utils :as test-utils]))

(defn linear-versions [setup teardown]
  (test-utils/with-temp-db store-spec setup teardown
    (let [foo-slice (slice/mk-slice [0 0 0] [4 5 6])
          foo-1 (test-utils/same-slab "int" foo-slice 1)
          foo-2a (test-utils/same-slab "int" foo-slice 2)
          foo-2b (test-utils/same-slab "int" foo-slice 3)

          dds {:dimensions {:x 4 :y 5 :z 6}
               :variables {:foo {:type "int" :shape ["x" "y" "z"] :fill-value -3}}
               :chunk-dimensions {:x 2 :y 2 :z 2}}

          ds-writer (db/dataset-writer store-spec)

          ;;; writing version 1
          writer-1 (-> ds-writer db/on-last-version (db/add-version dds))
          _ (with-open [foo-writer (-> writer-1 (db/variable-writer :foo))]
              (db/write foo-writer [foo-1]))
          _ (db/finish-version writer-1)
          _ (Thread/sleep 2000) ;; so the versions get different timestamps

          ;;; writing version 2a
          writer-2a (-> ds-writer db/on-last-version (db/add-version dds))
          writer-2b (-> ds-writer db/on-last-version (db/add-version dds))
          _ (with-open [foo-writer (-> writer-2a (db/variable-writer :foo))]
              (db/write foo-writer [foo-2a]))
          _ (db/finish-version writer-2a)

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

      (testing "data is returned properly for the version 1"
        (is (db/get-slice v1-foo-reader foo-slice))
        (is ((test-utils/same-as foo-1)
             (db/get-slice v1-foo-reader foo-slice))))

      (testing "data is returned as expected for the version 2a"
        (is (db/get-slice v2-foo-reader foo-slice))

        (is ((test-utils/same-as foo-2a)
             (db/get-slice v2-foo-reader foo-slice))))

      (testing "writing a second version to the same parent"
        ;; writing version 2b
        ;; using the same writer as 2a
        (with-open [foo-writer (-> writer-2b (db/variable-writer :foo))]
          (db/write foo-writer [foo-2b]))
        (is (thrown? Exception
                     (db/finish-version writer-2b)))))))
