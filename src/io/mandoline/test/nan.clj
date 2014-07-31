(ns io.mandoline.test.nan
  (:use
   [clojure test])
  (:require
   [io.mandoline :as db]
   [io.mandoline.slab :as slab]
   [io.mandoline.slice :as slice]
   [io.mandoline.test.utils :as test-utils]))

(defn fill-double [setup teardown]
  (test-utils/with-temp-db store-spec setup teardown
    (let [foo-slice (slice/mk-slice [0 0 0] [4 5 6])
          foo (test-utils/same-slab "double" foo-slice 1)
          entire-slice (slice/mk-slice [0 0 0] [5 6 7])
          entire-foo (-> (test-utils/same-slab "double" entire-slice Double/NaN)
                         (slab/merge foo))

          dds {:dimensions {:x 5 :y 6 :z 7}
               :variables {:foo {:type "double" :shape ["x" "y" "z"] :fill-value Double/NaN}}
               :chunk-dimensions {:x 2 :y 2 :z 2}}

          ds-writer (db/dataset-writer store-spec)

          ;;; writing version 1
          writer (-> ds-writer db/on-last-version (db/add-version dds))
          _ (with-open [foo-writer (-> writer (db/variable-writer :foo))]
              (db/write foo-writer [foo]))
          _ (db/finish-version writer)

          ;;; reading stuff
          foo-reader (-> store-spec db/dataset-reader db/on-last-version
                         (db/variable-reader :foo))]
      (testing "metadata is properly returned"
        (let [metadata (db/metadata store-spec)]
          (is (= (map :dimensions [metadata dds])))
          (is (= (map :variables [metadata dds])))
          (is (= (map :chunk-dimensions [metadata dds])))))

      (testing "data is returned properly with fill values (double)"
        (is (db/get-slice foo-reader foo-slice))
        (is ((test-utils/same-as foo)
             (db/get-slice foo-reader foo-slice)))

        (is (db/get-slice foo-reader entire-slice))
        (is ((test-utils/same-as entire-foo)
             (db/get-slice foo-reader entire-slice)))

        (is (thrown? IndexOutOfBoundsException
                     (db/get-slice foo-reader
                                   (slice/mk-slice [0 0 0] [6 6 6]))))))))

(defn fill-float [setup teardown]
  (test-utils/with-temp-db store-spec setup teardown
    (let [bar-slice (slice/mk-slice [0 0 0] [4 5 6])
          bar (test-utils/same-slab "float" bar-slice 1)
          entire-slice (slice/mk-slice [0 0 0] [5 6 7])
          entire-bar (-> (test-utils/same-slab "float" entire-slice Float/NaN)
                         (slab/merge bar))

          dds {:dimensions {:x 5 :y 6 :z 7}
                  :variables {:bar {:type "float" :shape ["x" "y" "z"] :fill-value Double/NaN}}
                  :chunk-dimensions {:x 2 :y 2 :z 2}}

          ds-writer (db/dataset-writer store-spec)

          ;;; writing version 1
          writer (-> ds-writer db/on-last-version (db/add-version dds))
          _ (with-open [bar-writer (-> writer (db/variable-writer :bar))]
              (db/write bar-writer [bar]))
          _ (db/finish-version writer)

          ;;; reading stuff
          versions (db/versions store-spec)
          first-time (-> versions ffirst (.plusMillis -1))

          bar-reader (-> store-spec db/dataset-reader db/on-last-version
                         (db/variable-reader :bar))]
      (testing "metadata is properly returned"
        (let [metadata (db/metadata store-spec)]
          (is (= (map :dimensions [metadata dds])))
          (is (= (map :variables [metadata dds])))
          (is (= (map :chunk-dimensions [metadata dds])))))

      (testing "data is returned properly with fill values (float)"
        (is (db/get-slice bar-reader bar-slice))
        (is ((test-utils/same-as bar)
             (db/get-slice bar-reader bar-slice)))

        (is (db/get-slice bar-reader entire-slice))
        (is ((test-utils/same-as entire-bar)
             (db/get-slice bar-reader entire-slice)))

        (is (thrown? IndexOutOfBoundsException
                     (db/get-slice bar-reader
                                   (slice/mk-slice [0 0 0] [6 6 6]))))))))

(defn fill-short [setup teardown]
  (test-utils/with-temp-db store-spec setup teardown
    (let [dds-short {:dimensions {:x 5 :y 6 :z 7}
                     :variables {:baz {:type "short"
                                       :shape ["x" "y" "z"]
                                       :fill-value Double/NaN}}
                     :chunk-dimensions {:x 2 :y 2 :z 2}}
          dds-int (assoc-in dds-short [:variables :baz :type] "int")
          dds-long (assoc-in dds-short [:variables :baz :type] "long")
          dds-byte (assoc-in dds-short [:variables :baz :type] "byte")
          dds-char (assoc-in dds-short [:variables :baz :type] "char")
          ds-writer (db/dataset-writer store-spec)]

      (testing "cannot write NaN fill values with shorts"
        (is (thrown? IllegalArgumentException
                     (-> ds-writer db/on-last-version (db/add-version dds-short)))))
      (testing "cannot write NaN fill values with ints"
        (is (thrown? IllegalArgumentException
                     (-> ds-writer db/on-last-version (db/add-version dds-int)))))
      (testing "cannot write NaN fill values with long"
        (is (thrown? IllegalArgumentException
                     (-> ds-writer db/on-last-version (db/add-version dds-long)))))
      (testing "cannot write NaN fill values with byte"
        (is (thrown? IllegalArgumentException
                     (-> ds-writer db/on-last-version (db/add-version dds-byte)))))
      (testing "cannot write NaN fill values with char"
        (is (thrown? IllegalArgumentException
                     (-> ds-writer db/on-last-version (db/add-version dds-char))))))))
