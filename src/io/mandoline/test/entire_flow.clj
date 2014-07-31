(ns io.mandoline.test.entire-flow
  (:use
   [clojure test])
  (:require
   [clojure.java.io :as jio]
   [io.mandoline :as db]
   [io.mandoline.slab :as slab]
   [io.mandoline.slice :as slice]
   [io.mandoline.utils :as utils]
   [io.mandoline.test.utils :as test-utils]))

(defn entire-flow [setup teardown]
  (test-utils/with-temp-db store-spec setup teardown
    (let [foo-1 (test-utils/random-slab "short" (slice/mk-slice [0 0 0] [3 5 8]) 10)
          foo-2 (test-utils/same-slab "short" (slice/mk-slice [3 5 8] [10 10 10]) 1)
          foo-3 (test-utils/random-slab "short" (slice/mk-slice [0 0 0] [3 5 8]) 10)
          foo-4 (test-utils/same-slab "short" (slice/mk-slice [3 5 8] [10 10 10]) 2)

          bar-1 (test-utils/random-slab "int" (slice/mk-slice [0 0 0 0] [2 3 4 5]) 10)
          bar-2 (test-utils/same-slab "int" (slice/mk-slice [2 3 4 5] [9 9 9 9]) 3)
          bar-3 (test-utils/random-slab "int" (slice/mk-slice [0 0 0 0] [2 3 4 5]) 10)
          bar-4 (test-utils/same-slab "int" (slice/mk-slice [2 3 4 5] [10 11 12 13]) 4)

          baz-1 (test-utils/same-slab "char" (slice/mk-slice [0] [10]) \a)
          baz-2 (test-utils/same-slab "char" (slice/mk-slice [10] [12]) \b)
          baz-3 (test-utils/same-slab "char" (slice/mk-slice [0] [10]) \c)
          baz-4 (test-utils/same-slab "char" (slice/mk-slice [10] [12]) \d)

          bork (test-utils/random-slab "float" (slice/mk-slice [0 0 0] [10 11 14]) 10)

          dds (-> (slurp (jio/resource "test-foobar.json"))
                  (utils/parse-metadata true))

          ds-writer (db/dataset-writer store-spec)

          ;;; writing version 1
          writer (-> ds-writer db/on-last-version (db/add-version dds))
          _ (with-open [foo-writer (-> writer (db/variable-writer :foo))]
              (db/write foo-writer [foo-1 foo-2]))
          _ (with-open [bar-writer (-> writer (db/variable-writer :bar))]
              (db/write bar-writer [bar-1 bar-2]))
          _ (with-open [baz-writer (-> writer (db/variable-writer :baz))]
              (db/write baz-writer [baz-1 baz-2]))
          _ (db/finish-version writer)

          _ (Thread/sleep 2000) ;; so the versions get different timestamps

          ;;; writing version 2
          writer (-> ds-writer (db/on-nth-version -1) (db/add-version dds))
          _ (with-open [foo-writer (-> writer (db/variable-writer :foo))]
              (db/write foo-writer [foo-3 foo-4]))
          _ (with-open [bar-writer (-> writer (db/variable-writer :bar))]
              (db/write bar-writer [bar-3 bar-4]))
          _ (with-open [baz-writer (-> writer (db/variable-writer :baz))]
              (db/write baz-writer [baz-3 baz-4]))
          _ (db/finish-version writer)

          _ (Thread/sleep 2000)

          ;;; writing version 3
          dds (-> dds
                  ;; add a new dimension
                  (assoc-in [:dimensions :q] 14)
                  (assoc-in [:chunk-dimensions :q] 2)
                  ;; add a new variable
                  (assoc-in [:variables :bork] {:type "float"
                                                :shape ["x" "y" "q"]
                                                :fill-value Float/NaN})
                  ;; change some existing metadata
                  (assoc-in [:variables :scalar :attributes :comments]
                            "Scalar, I am!"))
          writer (-> ds-writer (db/on-nth-version -1) (db/add-version dds))
          _ (with-open [bork-writer (-> writer (db/variable-writer :bork))]
              (db/write bork-writer [bork]))
          _ (db/finish-version writer)

          ;;; reading stuff
          fget-1 (slice/mk-slice [0 0 0] [2 2 2])
          fget-2 (slice/mk-slice [3 6 9] [4 7 10])
          bget-1 (slice/mk-slice [0 1 2 3] [2 3 4 5])
          bget-2 (slice/mk-slice [2 3 4 5] [6 7 8 9])
          bget-3 (slice/mk-slice [9 9 9 9] [10 11 12 13])
          bzget-1 (slice/mk-slice [0] [9])
          bzget-2 (slice/mk-slice [10] [12])
          bzget-3 (slice/mk-slice [11] [12])
          borkget (slice/mk-slice [0 0 0] [10 11 14])
          versions (db/versions store-spec)
          first-version (-> versions last second)
          second-version (-> versions second second)
          third-version (-> versions first second)
          first-time (-> versions second first (.plusMillis -1))

          v1-foo-reader (-> store-spec db/dataset-reader
                            (db/on-timestamp first-time)
                            (db/variable-reader :foo))
          v1-bar-reader (let [rdr (db/dataset-reader store-spec)
                              v (db/timestamp->version rdr first-time)]
                          (-> rdr (db/on-version v) (db/variable-reader :bar)))
          v1-baz-reader (-> store-spec db/dataset-reader
                            (db/on-nth-version -3)
                            (db/variable-reader :baz))
          v2-foo-reader (-> store-spec db/dataset-reader
                            (db/on-nth-version -2)
                            (db/variable-reader :foo))
          v2-bar-reader (-> store-spec db/dataset-reader
                            (db/on-nth-version -2)
                            (db/variable-reader :bar))
          v2-baz-reader (let [rdr (db/dataset-reader store-spec)
                              v (db/nth-version rdr -2)]
                          (-> rdr (db/on-version v) (db/variable-reader :baz)))
          v3-metadata (->> store-spec
                           db/last-version
                           (db/metadata store-spec))
          v3-foo-reader (-> store-spec db/dataset-reader
                            (db/on-nth-version -1)
                            (db/variable-reader :foo))
          v3-bar-reader (-> store-spec db/dataset-reader
                            (db/on-nth-version -1)
                            (db/variable-reader :bar))
          v3-baz-reader (let [rdr (db/dataset-reader store-spec)
                              v (db/nth-version rdr -1)]
                          (-> rdr (db/on-version v) (db/variable-reader :baz)))
          v3-bork-reader (-> store-spec db/dataset-reader
                             (db/on-last-version)
                             (db/variable-reader :bork))]

      (testing "making sure data is different"
        (is ((test-utils/same-as foo-1) foo-1))
        (is ((test-utils/same-as foo-2) foo-2))
        (is ((test-utils/same-as foo-3) foo-3))
        (is ((test-utils/same-as foo-4) foo-4))
        (is ((test-utils/same-as bar-1) bar-1))
        (is ((test-utils/same-as bar-2) bar-2))
        (is ((test-utils/same-as bar-3) bar-3))
        (is ((test-utils/same-as bar-4) bar-4))
        (is ((test-utils/same-as baz-1) baz-1))
        (is ((test-utils/same-as baz-2) baz-2))
        (is ((test-utils/same-as baz-3) baz-3))
        (is ((test-utils/same-as baz-4) baz-4))

        (is (not ((test-utils/same-as foo-1) foo-3)))
        (is (not ((test-utils/same-as foo-2) foo-4)))
        (is (not ((test-utils/same-as bar-1) bar-3)))
        (is (not ((test-utils/same-as bar-2) bar-4)))
        (is (not ((test-utils/same-as baz-1) baz-3)))
        (is (not ((test-utils/same-as baz-2) baz-4))))

      (testing "versions are referred to properly"
        (is (not (= (db/metadata store-spec first-version)
                    (db/metadata store-spec second-version))))
        (is (not (= (db/metadata store-spec second-version)
                    (db/metadata store-spec third-version))))
        (is (not (= (db/metadata store-spec first-version)
                    (db/metadata store-spec third-version))))
        (is (not (= "Scalar, I am!"
                    (-> (db/metadata store-spec first-version)
                        (get-in [:variables :scalar :attributes :comments])))))
        (is (not (= "Scalar, I am!"
                    (-> (db/metadata store-spec second-version)
                        (get-in [:variables :scalar :attributes :comments])))))
        (is (= "Scalar, I am!"
               (-> (db/metadata store-spec third-version)
                   (get-in [:variables :scalar :attributes :comments]))))
        (is (= (update-in (db/metadata store-spec third-version) [:variables :bork] dissoc :fill-value)
               (update-in (db/metadata store-spec) [:variables :bork] dissoc :fill-value)))
        (is (utils/nan= (get-in (db/metadata store-spec third-version) [:variables :bork :fill-value])
                        (get-in (db/metadata store-spec) [:variables :bork :fill-value])))
        (is (nil? (-> (db/metadata store-spec first-version)
                      (get-in [:dimensions :q]))))
        (is (nil? (-> (db/metadata store-spec second-version)
                      (get-in [:dimensions :q]))))
        (is (= 14 (-> (db/metadata store-spec third-version)
                      (get-in [:dimensions :q]))))
        (is (nil? (-> (db/metadata store-spec first-version)
                      (get-in [:chunk-dimensions :q]))))
        (is (nil? (-> (db/metadata store-spec second-version)
                      (get-in [:chunk-dimensions :q]))))
        (is (= 2 (-> (db/metadata store-spec third-version)
                     (get-in [:chunk-dimensions :q]))))
        (is (nil? (-> (db/metadata store-spec first-version)
                      (get-in [:variables :bork]))))
        (is (nil? (-> (db/metadata store-spec first-version)
                      (get-in [:variables :bork])))))

      (testing "indices are different between versions"
        (for [var [:foo :bar :baz]]
          (is (not (= (get-in (db/metadata store-spec first-version)
                              [:variables var :index])
                      (get-in (db/metadata store-spec second-version)
                              [:variables var :index]))))))

      (testing "data is returned properly for the first version"
        (is (db/get-slice v1-foo-reader fget-1))
        (is ((test-utils/same-as (slab/subset foo-1 fget-1))
             (db/get-slice v1-foo-reader fget-1)))

        (is (db/get-slice v1-foo-reader fget-2))
        (is ((test-utils/same-as (slab/subset foo-2 fget-2))
             (db/get-slice v1-foo-reader fget-2)))

        (is (db/get-slice v1-bar-reader bget-1))
        (is (slab/subset bar-1 bget-1))
        (is ((test-utils/same-as (slab/subset bar-1 bget-1))
             (db/get-slice v1-bar-reader bget-1)))

        (is (db/get-slice v1-bar-reader bget-2))
        (is (slab/subset bar-2 bget-2))
        (is ((test-utils/same-as (slab/subset bar-2 bget-2))
             (db/get-slice v1-bar-reader bget-2)))

        (is (db/get-slice v1-bar-reader bget-3))
        (is ((test-utils/same-as (test-utils/same-slab "int" bget-3 -99))
             (db/get-slice v1-bar-reader bget-3)))

        (is (db/get-slice v1-baz-reader bzget-1))
        (is (slab/subset baz-1 bzget-1))
        (is ((test-utils/same-as (slab/subset baz-1 bzget-1))
             (db/get-slice v1-baz-reader bzget-1)))

        (is (db/get-slice v1-baz-reader bzget-2))
        (is (slab/subset baz-2 bzget-2))
        (is ((test-utils/same-as (slab/subset baz-2 bzget-2))
             (db/get-slice v1-baz-reader bzget-2)))

        (is (db/get-slice v1-baz-reader bzget-3))
        (is ((test-utils/same-as (test-utils/same-slab "char" bzget-3 \b))
             (db/get-slice v1-baz-reader bzget-3))))

      (testing "data is returned as expected for the second version"
        (is (db/get-slice v2-foo-reader fget-1))
        (is ((test-utils/same-as (slab/subset foo-3 fget-1))
             (db/get-slice v2-foo-reader fget-1)))

        (is (db/get-slice v2-foo-reader fget-2))
        (is ((test-utils/same-as (slab/subset foo-4 fget-2))
             (db/get-slice v2-foo-reader fget-2)))

        (is (db/get-slice v2-bar-reader bget-1))
        (is ((test-utils/same-as (slab/subset bar-3 bget-1))
             (db/get-slice v2-bar-reader bget-1)))

        (is (db/get-slice v2-bar-reader bget-2))
        (is ((test-utils/same-as (slab/subset bar-4 bget-2))
             (db/get-slice v2-bar-reader bget-2)))

        (is (db/get-slice v2-bar-reader bget-3))
        (is ((test-utils/same-as (slab/subset bar-4 bget-3))
             (db/get-slice v2-bar-reader bget-3)))

        (is (db/get-slice v2-baz-reader bzget-1))
        (is ((test-utils/same-as (slab/subset baz-3 bzget-1))
             (db/get-slice v2-baz-reader bzget-1)))

        (is (db/get-slice v2-baz-reader bzget-2))
        (is ((test-utils/same-as (slab/subset baz-4 bzget-2))
             (db/get-slice v2-baz-reader bzget-2)))

        (is (db/get-slice v2-baz-reader bzget-3))
        (is ((test-utils/same-as (slab/subset baz-4 bzget-3))
             (db/get-slice v2-baz-reader bzget-3))))

      (testing "data is returned as expected for the third version"
        (is (db/get-slice v3-foo-reader fget-1))
        (is ((test-utils/same-as (slab/subset foo-3 fget-1))
             (db/get-slice v3-foo-reader fget-1)))

        (is (db/get-slice v3-foo-reader fget-2))
        (is ((test-utils/same-as (slab/subset foo-4 fget-2))
             (db/get-slice v3-foo-reader fget-2)))

        (is (db/get-slice v3-bar-reader bget-1))
        (is ((test-utils/same-as (slab/subset bar-3 bget-1))
             (db/get-slice v3-bar-reader bget-1)))

        (is (db/get-slice v3-bar-reader bget-2))
        (is ((test-utils/same-as (slab/subset bar-4 bget-2))
             (db/get-slice v3-bar-reader bget-2)))

        (is (db/get-slice v3-bar-reader bget-3))
        (is ((test-utils/same-as (slab/subset bar-4 bget-3))
             (db/get-slice v3-bar-reader bget-3)))

        (is (db/get-slice v3-baz-reader bzget-1))
        (is ((test-utils/same-as (slab/subset baz-3 bzget-1))
             (db/get-slice v3-baz-reader bzget-1)))

        (is (db/get-slice v3-baz-reader bzget-2))
        (is ((test-utils/same-as (slab/subset baz-4 bzget-2))
             (db/get-slice v3-baz-reader bzget-2)))

        (is (db/get-slice v3-baz-reader bzget-3))
        (is ((test-utils/same-as (slab/subset baz-4 bzget-3))
             (db/get-slice v3-baz-reader bzget-3)))

        (is (db/get-slice v3-bork-reader borkget))
        (is ((test-utils/same-as (slab/subset bork borkget))
             (db/get-slice v3-bork-reader borkget)))))))
