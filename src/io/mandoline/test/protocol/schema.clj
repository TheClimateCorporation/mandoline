(ns io.mandoline.test.protocol.schema
  "Test helpers for testing implementations of the Mandoline Schema
  protocol."
  (:require
    [clojure.test :refer :all]
    [clojure.test.check.generators :as gen]
    [io.mandoline.impl.protocol :as proto]))

(defn- generate-strings
  "Create a finite seq of distinct non-empty alphanumeric strings.

  If an optional collection argument is provided, then the seq will not
  contain any elements that belong to this collection."
  ([n]
   (generate-strings n #{}))
  ([n coll]
   (let [exclude (set coll)]
     (->> gen/string-alpha-numeric
       (gen/not-empty)
       (gen/sample-seq)
       (remove exclude)
       (distinct)
       (take n)))))

(defn- generate-non-strings
  "Create a finite seq of pseudorandom non-string values."
  [n]
  (let [max-size 4]
    (take n (gen/sample-seq
              (gen/such-that (complement string?) gen/any)
              max-size))))

(defn- coll-is-equivalent-to-set
  "Return true if a collection contains distinct elements that match the
  elements of an expected set.

  This function is invariant to the order of elements in the collection,
  but it is sensitive to any missing or duplicated elements."
  [coll expected-set]
  {:pre [(set? expected-set)]}
  (and (= (count coll) (count expected-set)) (= (set coll) expected-set)))

(defn- test-create-dataset-single-threaded
  "Test Schema properties with sequential calls to the create-dataset
  method in a single thread.

  This function is intended for its side effects through the
  clojure.test/is assertion macro. Do not rely upon the return value."
  [schema datasets-to-create]
  (testing "create-dataset method mutates the list of datasets"
    (doseq [name datasets-to-create
            :let [before (set (proto/list-datasets schema))]]
      (is (nil? (proto/create-dataset schema name))
          "create-dataset method returns nil")
      (is (coll-is-equivalent-to-set
            (proto/list-datasets schema)
            (conj before name))
          "create-dataset method adds exactly one new dataset")))
  (testing "create-dataset method throws exception on invalid argument"
    (doseq [name (proto/list-datasets schema)]
      (is (thrown? Exception (proto/create-dataset schema name))
          "Calling create-dataset with existing dataset throws exception"))
    (doseq [name (generate-non-strings 100)]
      (is (thrown? Exception (proto/create-dataset schema name))
          "Calling create-dataset with non-string name throws exception"))
    (is (thrown? Exception (proto/create-dataset schema ""))
        "Calling create-datset with empty string name throws exception")))

(defn- test-list-datasets-single-threaded
  "Test Schema properties with sequential calls to the list-datasets
  method in a single thread.

  This function is intended for its side effects through the
  clojure.test/is assertion macro. Do not rely upon the return value."
  [schema]
  (testing
    "list-datasets method returns a list of distinct valid dataset names"
    (let [listed-datasets (proto/list-datasets schema)]
      (is (seq? listed-datasets)
          "list-datasets method returns a seq")
      (is (every? string? listed-datasets)
          "list-datasets method returns a collection of strings")
      (is (every? not-empty listed-datasets)
          "list-datasets method returns non-empty elements")
      (is (or (empty? listed-datasets) (apply distinct? listed-datasets))
          "list-dataset method returns distinct elements"))))

(defn- test-connect-single-threaded
  "Test Schema properties with sequential calls to the connect method in
  a single thread.

  This function is intended for its side effects through the
  clojure.test/is assertion macro. Do not rely upon the return value."
  [schema]
  (let [existing-datasets (proto/list-datasets schema)]
    (testing
      "connect method returns a connection to a dataset"
      (doseq [name existing-datasets]
        (is (satisfies? proto/Connection (proto/connect schema name))
            "connect method returns a Connection instance"))
        (is (= existing-datasets (proto/list-datasets schema))
            "Calling connect method does not mutate list of datasets"))
    (testing
      "connect method throws exception when dataset does not exist"
      (doseq [name (generate-strings 10 existing-datasets)]
        (is (thrown? Exception (proto/connect schema name))
            "connect method throws exception when the dataset does not exist")))))

(defn- test-destroy-dataset-single-threaded
  "Test Schema properties with sequential calls to the destroy-dataset
  method in a single thread.

  This function is intended for its side effects through the
  clojure.test/is assertion macro. Do not rely upon the return value."
  [schema datasets-to-destroy]
  (testing
    "destroy-dataset method mutates the list of datasets"
    (doseq [name datasets-to-destroy
            :let [before (set (proto/list-datasets schema))]]
      (is (nil? (proto/destroy-dataset schema name))
          "destroy-dataset method returns nil")
      (is (coll-is-equivalent-to-set
            (proto/list-datasets schema)
            (disj before name))
          "destroy-dataset method removes exactly the specified dataset")))
  (testing
    "destroy-dataset method is idempotent"
    (let [existing-datasets (set (proto/list-datasets schema))]
      (doseq [name (generate-strings 10 existing-datasets)]
        (is (nil? (proto/destroy-dataset schema name))
            "destroy-dataset method returns nil for a nonexistent dataset")
        (is (coll-is-equivalent-to-set
              (proto/list-datasets schema)
              existing-datasets)
            (str "list of datasets is unchanged after calling destroy-dataset "
                 "method on nonexistent dataset"))))))

(defn- test-create-dataset-multi-threaded
  "Test Schema properties with concurrent calls to the create-dataset
  method in multiple threads.

  This function is intended for its side effects through the
  clojure.test/is assertion macro. Do not rely upon the return value."
  [schema datasets-to-create]
  (testing "create-dataset method exhibits expected concurrency behavior"
    (let [initial-datasets (set (proto/list-datasets schema))
          as (->> datasets-to-create ; (a b c ...)
               (shuffle) ; (c x b ...)
               (map #(repeat 3 %)) ; ((c c c) (x x x) (b b b) ...)
               (apply concat) ; (c c c x x x b b b ...)
               (map agent))
          f (fn [state]
              (try
                (proto/create-dataset schema state)
                {:name state :success? true}
                (catch Exception _
                  {:name state :success? false})))
          results (do
                    ; Send action to agents
                    (doseq [a as] (send a f))
                    ; Wait for all actions to be completed
                    (apply await as)
                    ; Get results
                    (map deref as))]
      (is (coll-is-equivalent-to-set
            (map :name (filter :success? results))
            (set datasets-to-create))
          (str "concurrent create-dataset method calls succeed exactly "
               "once for each distinct dataset name"))
      (is (coll-is-equivalent-to-set
            (proto/list-datasets schema)
            (reduce conj initial-datasets datasets-to-create))
          (str "list of datasets was correctly updated by concurrent "
               "create-dataset method calls")))))

(defn- test-list-datasets-multi-threaded
  "Test Schema properties with concurrent calls to the list-datasets
  method in multiple threads.

  This function is intended for its side effects through the
  clojure.test/is assertion macro. Do not rely upon the return value."
  [schema]
  (testing "list-datasets method is consistent when called concurrently"
    (let [initial-datasets (set (proto/list-datasets schema))
          as (map agent (range 100))
          f (fn [state]
              (proto/list-datasets schema))
          results (do
                    ; Send action to agents
                    (doseq [a as] (send a f))
                    ; Wait for all actions to be completed (any
                    ; exceptions will be propagated)
                    (apply await as)
                    ; Get results
                    (map deref as))]
      (is (every? #(= initial-datasets %) (map set results))
          (str "concurrent calls to the list-datasets method return "
               "equivalent results")))))

(defn- test-connect-multi-threaded
  "Test Schema properties with concurrent calls to the connect method in
  multiple threads.

  This function is intended for its side effects through the
  clojure.test/is assertion macro. Do not rely upon the return value."
  [schema]
  (testing "connect method can be called concurrently for the same dataset"
    (let [existing-datasets (set (proto/list-datasets schema))
          f (fn [state]
              {:name state
               :connection (proto/connect schema state)})
          as (->> existing-datasets ; (a b c ...)
               (shuffle) ; (c x b ...)
               (map #(repeat 3 %)) ; ((c c c) (x x x) (b b b) ...)
               (apply concat) ; (c c c x x x b b b ...)
               (map agent))
          results (do
                    ; Send action to agents
                    (doseq [a as] (send a f))
                    ; Wait for all actions to be completed (any
                    ; exceptions will be propagated)
                    (apply await as)
                    ; Get results
                    (map deref as))]
      (is (every? #(satisfies? proto/Connection (:connection %)) results)
          "concurrent connect method calls return Connection instances")
      ; This assert is commented out because Connection does not
      ; implement the equals method.
      #_(is (every?
            #(apply = %)
            (for [[_ r] (group-by :name results)] (map :connection r)))
          (str "concurrent connect method calls with the same dataset name "
               "return equivalent Connection instances"))
      (is (coll-is-equivalent-to-set
            (proto/list-datasets schema)
            existing-datasets)
          "list of datasets is unchanged by calls to the connect method"))))

(defn- test-destroy-dataset-multi-threaded
  "Test Schema properties with concurrent calls to the destroy-dataset
  method in multiple threads.

  This function is intended for its side effects through the
  clojure.test/is assertion macro. Do not rely upon the return value."
  [schema datasets-to-destroy]
  (testing "destroy-dataset method exhibits expected concurrency behavior"
    (let [initial-datasets (set (proto/list-datasets schema))
          as (->> datasets-to-destroy ; (a b c ...)
               (shuffle) ; (c x b ...)
               (map #(repeat 3 %)) ; ((c c c) (x x x) (b b b) ...)
               (apply concat) ; (c c c x x x b b b ...)
               (map agent))
          f (fn [state]
              (proto/destroy-dataset schema state))
          results (do
                    ; Send action to agents
                    (doseq [a as] (send a f))
                    ; Wait for all actions to be completed (any
                    ; exceptions will be propagated)
                    (apply await as)
                    ; Get results
                    (map deref as))]
      (is (every? nil? results)
          "concurrent destroy-dataset method calls return nil")
      (is (coll-is-equivalent-to-set
            (proto/list-datasets schema)
            (reduce disj initial-datasets datasets-to-destroy))
          (str "list of datasets was correctly updated by concurrent "
               "destroy-dataset method calls")))))

(defn test-schema-properties-single-threaded
  "Given a zero-argument setup function that returns a Schema instance
  and a one-argument teardown function that cleans up this Schema
  instance, calls schema methods sequentially in a single thread and
  check properties of a Schema implementation.

  The caller is responsible for providing well-behaved setup and
  teardown arguments so that side effects are not persisted after this
  function returns.

  This function is intended for its side effects through the
  clojure.test/is assertion macro. Do not rely upon the return value.

  Usage example:

      (deftest my-schema-implementation-single-threaded-test
        (let [setup (fn [] (->MySchema \"root\"))
              teardown (fn [schema] (reset schema))]
              num-datasets 10]
          (test-schema-properties-single-threaded setup teardown num-datasets)))
  "
  [setup teardown num-datasets]
  (let [schema (setup)]
    (try
      (testing
        (format "Single-threaded tests of Schema %s:" (type schema))
        (is (satisfies? proto/Schema schema)
            (str (type schema) " instance implements the Schema protocol"))
        (test-list-datasets-single-threaded schema)
        (let [initial-datasets (proto/list-datasets schema)
              test-datasets (generate-strings num-datasets initial-datasets)]
          (test-list-datasets-single-threaded schema)
          (test-create-dataset-single-threaded schema test-datasets)
          (test-list-datasets-single-threaded schema)
          (test-connect-single-threaded schema)
          (test-destroy-dataset-single-threaded schema (shuffle test-datasets))
          (test-list-datasets-single-threaded schema)
          (test-connect-single-threaded schema)))
      (finally (teardown schema)))))

(defn test-schema-properties-multi-threaded
  "Given a zero-argument setup function that returns a Schema instance
  and a one-argument teardown function that cleans up this Schema
  instance, calls schema methods concurrently in multiple threads and
  check properties of a Schema implementation.

  The caller is responsible for providing well-behaved setup and
  teardown arguments so that side effects are not persisted after this
  function returns.

  This function is intended for its side effects through the
  clojure.test/is assertion macro. Do not rely upon the return value.

  Usage example:

      (deftest my-schema-implementation-multi-threaded-test
        (let [setup (fn [] (->MySchema \"root\"))
              teardown (fn [schema] (reset schema))
              num-datasets 10]
          (test-schema-properties-multi-threaded setup teardown num-datasets)))
  "
  [setup teardown num-datasets]
  (let [schema (setup)]
    (try
      (testing
        (format "Multi-threaded tests of Schema %s:" (type schema))
        (let [initial-datasets (proto/list-datasets schema)
              test-datasets (generate-strings num-datasets initial-datasets)]
          (test-create-dataset-multi-threaded schema test-datasets)
          (test-list-datasets-multi-threaded schema)
          (test-connect-multi-threaded schema)
          (test-destroy-dataset-multi-threaded schema test-datasets)))
      (finally (teardown schema)))))
