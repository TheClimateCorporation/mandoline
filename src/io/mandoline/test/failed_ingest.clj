(ns io.mandoline.test.failed-ingest
  (:use
   [clojure test])
  (:require
   [io.mandoline :as db]
   [io.mandoline.slab :as slab]
   [io.mandoline.slice :as slice]
   [io.mandoline.utils :as utils]
   [io.mandoline.test.utils :as test-utils]
   [clojure.java.io :as jio]))

(def data1 (test-utils/same-slab "short" (slice/mk-slice [0 0 0] [10 10 10]) 1))
(def data2 (test-utils/same-slab "short" (slice/mk-slice [0 0 0] [10 10 10]) 2))
(def data3 (test-utils/same-slab "short" (slice/mk-slice [0 0 0] [1 1 1]) 3))
(defn dds []
  (-> (slurp (jio/resource "test-foobar.json"))
      (utils/parse-metadata true)
      (assoc-in [:chunk-dimensions :x] 3)
      (assoc-in [:chunk-dimensions :y] 3)
      (assoc-in [:chunk-dimensions :z] 3)
      (assoc-in [:chunk-dimensions :time] 3)))

(defn writer [spec]
  (-> spec db/dataset-writer db/on-last-version (db/add-version (dds))))

(defn write|write-no-commit|write [setup teardown]
  (test-utils/with-temp-db store-spec setup teardown
    (let [w (writer store-spec)
          ;;; writing version 1
          _ (with-open [foo-writer (-> w (db/variable-writer :foo))]
              (db/write foo-writer [data1]))
          _ (db/finish-version w)

          _ (Thread/sleep 2000) ;; so the versions get different timestamps

          ;;; writing incomplete version
          w (writer store-spec)
          _ (with-open [foo-writer (-> w (db/variable-writer :foo))]
              (db/write foo-writer [data2]))
          ;; notice we don't finish-version


          _ (Thread/sleep 100) ;; so the versions get different timestamps

          ;;; writing version 3
           w (writer store-spec)
          _ (with-open [foo-writer (-> w (db/variable-writer :foo))]
              (db/write foo-writer [data3]))
          _ (db/finish-version w)

          versions (db/versions store-spec)

          ;;; reading stuff
          fget-1 (slice/mk-slice [0 0 0] [5 5 5])
          v1-foo-reader (-> store-spec db/dataset-reader
                            (db/on-version (-> versions last last))
                            (db/variable-reader :foo))
          v2-foo-reader (-> store-spec db/dataset-reader
                            (db/on-last-version)
                            (db/variable-reader :foo))]

      (testing
        (is (= 2 (count versions)))
        (is ((test-utils/same-as (slab/subset data1 fget-1))
             (db/get-slice v1-foo-reader fget-1)))
        (is ((test-utils/same-as (slab/merge (slab/subset data1 fget-1) data3))
             (db/get-slice v2-foo-reader fget-1)))))))

(defn write|write-no-close|write [setup teardown]
  (test-utils/with-temp-db store-spec setup teardown
    (let [w (writer store-spec)
          ;;; writing version 1
          _ (with-open [foo-writer (-> w (db/variable-writer :foo))]
              (db/write foo-writer [data1]))
          _ (db/finish-version w)

          _ (Thread/sleep 2000) ;; so the versions get different timestamps

          ;;; writing incomplete version
          w (writer store-spec)
          _ (let [foo-writer (-> w (db/variable-writer :foo))]
              (db/write foo-writer [data2]))
          ;; notice we don't finish-version and we don't close the variable-writer


          _ (Thread/sleep 100) ;; so the versions get different timestamps

          ;;; writing version 3
           w (writer store-spec)
          _ (with-open [foo-writer (-> w (db/variable-writer :foo))]
              (db/write foo-writer [data3]))
          _ (db/finish-version w)

          versions (db/versions store-spec)

          ;;; reading stuff
          fget-1 (slice/mk-slice [0 0 0] [5 5 5])
          v1-foo-reader (-> store-spec db/dataset-reader
                            (db/on-version (-> versions last last))
                            (db/variable-reader :foo))
          v2-foo-reader (-> store-spec db/dataset-reader
                            (db/on-last-version)
                            (db/variable-reader :foo))]

      (testing
        (is (= 2 (count versions)))
        (is ((test-utils/same-as (slab/subset data1 fget-1))
             (db/get-slice v1-foo-reader fget-1)))
        (is ((test-utils/same-as (slab/merge (slab/subset data1 fget-1) data3))
             (db/get-slice v2-foo-reader fget-1)))))))

(defn write|write-no-commit|write-no-commit [setup teardown]
  (test-utils/with-temp-db store-spec setup teardown
    (let [w (writer store-spec)
          ;;; writing version 1
          _ (with-open [foo-writer (-> w (db/variable-writer :foo))]
              (db/write foo-writer [data1]))
          _ (db/finish-version w)

          _ (Thread/sleep 2000) ;; so the versions get different timestamps

          ;;; writing incomplete version
          w (writer store-spec)
          _ (with-open [foo-writer (-> w (db/variable-writer :foo))]
              (db/write foo-writer [data2]))
          ;; notice we don't finish-version

          _ (Thread/sleep 100) ;; so the versions get different timestamps

          ;;; writing incomplete version
          w (writer store-spec)
          _ (with-open [foo-writer (-> w (db/variable-writer :foo))]
              (db/write foo-writer [data3]))
          ;; notice we don't finish-version

          versions (db/versions store-spec)

          ;;; reading stuff
          foo-reader (-> store-spec db/dataset-reader
                         (db/on-last-version)
                         (db/variable-reader :foo))]

      (testing
        (is (= 1 (count versions)))
        (is ((test-utils/same-as data1)
             (db/get-slice foo-reader (slice/mk-slice [0 0 0] [10 10 10]))))))))

(defn failed-write [setup teardown]
  (write|write-no-commit|write setup teardown)
  (write|write-no-close|write setup teardown))
