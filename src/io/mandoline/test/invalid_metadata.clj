(ns io.mandoline.test.invalid-metadata
  (:use
   [clojure test])
  (:require
   [io.mandoline :as db]
   [io.mandoline.slice :as slice]
   [io.mandoline.test.utils :as test-utils]))

(defn invalid-metadata [setup teardown]
  (test-utils/with-temp-db store-spec setup teardown
    (let [dds {:dimensions {:x 4 :y 5 :z 6}
               :variables {:foo {:type "double" :shape ["x" "y" "z"] :fill-value -3}}
               :chunk-dimensions {:x 2 :y 2 :z 2}}
          dds-1 (assoc-in dds [:variables :foo :shape] ["foo"])
          dds-2 (assoc-in dds [:variables :foo :type] "unsupported-type")
          dds-3 (assoc dds :chunk-dimensions {:x 2 :y 2})
          dds-4 (dissoc dds :dimensions)
          dds-5 (dissoc dds :variables)
          dds-6 (dissoc dds :chunk-dimensions)
          dds-7 (assoc-in dds [:variables :foo :shape] nil)
          dds-8 (assoc-in dds [:variables :foo :type] nil)
          dds-9 (assoc-in dds [:variables :foo :fill-value] nil)
          dds-10 (assoc-in dds [:variables :foo :fill-value] "unsupported-fill")
          dds-11 (assoc-in dds [:variables :foo :fill-value] Double/NaN)
          ds-writer (db/dataset-writer store-spec)
          before-writing (db/on-last-version ds-writer)]
      (is (thrown? IllegalArgumentException (db/add-version before-writing dds-1)))
      (is (thrown? IllegalArgumentException (db/add-version before-writing dds-2)))
      (is (thrown? IllegalArgumentException (db/add-version before-writing dds-3)))
      (is (thrown? IllegalArgumentException (db/add-version before-writing dds-4)))
      (is (thrown? IllegalArgumentException (db/add-version before-writing dds-5)))
      (is (thrown? IllegalArgumentException (db/add-version before-writing dds-6)))
      (is (thrown? IllegalArgumentException (db/add-version before-writing dds-7)))
      (is (thrown? IllegalArgumentException (db/add-version before-writing dds-8)))
      (is (thrown? IllegalArgumentException (db/add-version before-writing dds-9)))
      (is (thrown? IllegalArgumentException (db/add-version before-writing dds-10)))
      (is (db/add-version before-writing dds-11)))))

(defn change-metadata [setup teardown]
  (test-utils/with-temp-db store-spec setup teardown
    (let [dds {:dimensions {:x 4 :y 5 :z 6}
               :variables {:foo {:type "short" :shape ["x" "y" "z"] :fill-value -3}}
               :chunk-dimensions {:x 2 :y 2 :z 2}}
          child-1 (assoc dds :chunk-dimensions {:x 2 :y 2 :z 3})
          child-2 (assoc-in dds [:variables :foo :shape] ["z" "x" "y"])
          child-3 (assoc-in dds [:variables :foo :type] "int")
          child-4 (assoc-in dds [:variables :foo :fill-value] -99)

          foo (test-utils/same-slab "short" (slice/mk-slice [0 0 0] [4 5 6]) 1)
          ds-writer (db/dataset-writer store-spec)
          writer (-> ds-writer db/on-last-version (db/add-version dds))
          _ (with-open [foo-writer (-> writer (db/variable-writer :foo))]
              (db/write foo-writer [foo]))
          _ (db/finish-version writer)

          _ (Thread/sleep 2000) ;; so we get the written version
          after-writing (db/on-last-version ds-writer)]
      (is (thrown? IllegalArgumentException (db/add-version after-writing child-1)))
      (is (thrown? IllegalArgumentException (db/add-version after-writing child-2)))
      (is (thrown? IllegalArgumentException (db/add-version after-writing child-3)))
      (is (thrown? IllegalArgumentException (db/add-version after-writing child-4))))))
