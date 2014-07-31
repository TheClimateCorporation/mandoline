(ns io.mandoline.test.utils-test
  (:require
    [clojure.test :refer :all]
    [io.mandoline :as mandoline]
    [io.mandoline.backend.mem :as mem]
    [io.mandoline.impl.protocol :refer [Schema]]
    [io.mandoline.test.utils :as utils]))

(deftest test-setup-and-teardown-mem-spec
  (testing "setup-mem-spec and teardown-mem-spec work together"
    (is (map? (utils/setup-mem-spec))
        "setup-mem-spec returns a map")
    (is (satisfies? Schema (mem/mk-schema (utils/setup-mem-spec)))
        (str
          "setup-mem-spec returns a valid store spec argument to "
          "io.mandoline.backend.mem/mk-schema"))
    (is (nil? (utils/teardown-mem-spec (utils/setup-mem-spec)))
        (str
          "teardown-mem-spec returns nil when called on the spec map that "
          "is returned by setup-mem-spec"))
    (let [spec (utils/setup-mem-spec)
          name (utils/random-name)]
      (.create-dataset (mem/mk-schema spec) name)
      (utils/teardown-mem-spec spec)
      (is (thrown? Exception (.connect (mem/mk-schema spec) name))
          "teardown-mem-spec successfully deletes stored data"))))

(deftest test-with-temp-db-macro-minimal
  (testing "with-temp-db macro works with the in-memory Mandoline store"
    (let [root (utils/random-name)
          dataset (utils/random-name)
          expected-store-spec (atom nil)
          setup (fn []
                  (let [schema (-> (mem/create-schema root)
                                 (get root)
                                 (mem/->MemSchema))
                        store-spec {:schema schema
                                    :dataset dataset
                                    :another-key :another-value}]
                    (swap! expected-store-spec (constantly store-spec))
                    store-spec))
          teardown (fn [store-spec]
                     (is (= store-spec @expected-store-spec)
                         (str "with-temp-db calls teardown function "
                              "with the expected store spec argument"))
                     (mem/destroy-schema (:schema store-spec)))]
      (utils/with-temp-db test-store-spec setup teardown
        (is (= test-store-spec @expected-store-spec)
            (str "with-temp-db binds symbol to the store spec that is "
                 "returned by the setup function"))
        (is (= (mandoline/list-datasets test-store-spec) (list dataset))
            (str "with-temp-db macro automatically creates a dataset")))
      ; The dataset that was created by the with-temp-db macro is
      ; automatically destroyed after the body is executed.
      (is (empty? (mandoline/list-datasets (setup)))
          (str "Temporary dataset is automatically destroyed after "
               "the body is executed within the with-temp-db macro")))))
