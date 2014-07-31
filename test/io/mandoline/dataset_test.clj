(ns io.mandoline.dataset-test
  (:require
    [clojure.test :refer :all]
    [io.mandoline.dataset :as dataset]))

;; Borrowed from Chris Houser, public domain
(defmacro with-private-fns
  "Alias private functions from namespace ns into the current namespace, for the
  duration of tests."
  [[ns fns] & tests]
  "Refers private fns from ns and runs tests in context."
  `(let ~(reduce #(conj %1 %2 `(ns-resolve '~ns '~%2)) [] fns)
     ~@tests))

(def supported-types
  #{"byte" "char" "short" "int" "long" "float" "double"})

(deftest validate-dataset-definition-test
  (let [valid? (fn [d] (nil? (dataset/validate-dataset-definition d)))
        invalid? (fn [d] (try (not (nil? (dataset/validate-dataset-definition d)))
                             (catch IllegalArgumentException e
                               true)))
        m {:dimensions {:x 100
                        :y 200
                        :z 300}
           :variables {:foo {:type "short"
                             :shape [:x :y :z]
                             :fill-value -1}
                       :bar {:type "double"
                             :shape [:z :y :x]
                             :fill-value Double/NaN}}
           :chunk-dimensions {:x 10
                              :y 10
                              :z 10}}]
    (testing "validate-dataset-definition"
      (is (valid? m))
      (is (valid? (update-in m [:variables] dissoc :bar))))
    (testing "validate-dataset-definition minimum keys"
      (for [k [:dimensions :variables :chunk-dimensions]]
        (is (invalid? (dissoc m k)))))
    (testing "validate-dataset-definition chunk-dims == dims"
      (is (invalid? (update-in m [:dimensions] dissoc :x)))
      (is (invalid? (update-in m [:chunk-dimensions] dissoc :x))))
    (testing "validate-dataset-definition variable structure"
      (is (invalid? (update-in m [:variables :foo] dissoc :type)))
      (is (invalid? (update-in m [:variables :foo] dissoc :shape)))
      (is (invalid? (update-in m [:variables :foo] dissoc :fill-value)))
      ;; dimensionless vars are valid
      (is (valid? (update-in m [:variables :foo] assoc :shape []))))
    (testing "validate-dataset-definition types"
      ;; supported types
      (doseq [t supported-types]
        (is (valid? (update-in m [:variables :foo]
                              assoc :type t))))
      ;; unsupported types
      (doseq [t ["bit" "bool" "boolean" "ubyte" "ushort"
                 "uint" "ulong" "int64" "uint64" "string"]]
        (is (invalid? (update-in m [:variables :foo]
                                 assoc :type t)))))
    (testing "validate-dataset-definition fill-values"
      (is (invalid? (update-in m [:variables :foo] assoc :fill-value Double/NaN)))
      (is (invalid? (update-in m [:variables :foo] assoc :fill-value "bar")))
      (is (valid? (update-in m [:variables :foo] assoc :fill-value 0))))))

(deftest validate-var-test
  (with-private-fns [io.mandoline.dataset [validate-var-type]]
    (testing "validate-var-type"
      (let [valid? (fn [p c v] (nil? (validate-var-type p c v)))
            invalid? (fn [p c v] (try (not (nil? (validate-var-type p c v)))
                                     (catch IllegalArgumentException e
                                       true)))]
        (doseq [t supported-types]
          (is (valid? {:variables {"boils" {:type t}}}
                      {:variables {"boils" {:type t}}}
                      "boils")))
        (doseq [t supported-types
              n (disj supported-types t)]
          (is (invalid? {:variables {"gnats" {:type "float"}}}
                        {:variables {"gnats" {:type "int"}}}
                        "gnats"))))))
  (with-private-fns [io.mandoline.dataset [validate-var-shape]]
    (testing "validate-var-shape"
      (let [valid? (fn [p c v] (nil? (validate-var-shape p c v)))
            invalid? (fn [p c v] (try (not (nil? (validate-var-shape p c v)))
                                     (catch IllegalArgumentException e
                                       true)))]
      (is (valid? {:variables {"boils" {:shape ["x", "y", "time"]}}}
                  {:variables {"boils" {:shape ["x", "y", "time"]}}}
                  "boils"))
      (is (invalid? {:variables {"gnats" {:shape ["x", "y", "time"]}}}
                    {:variables {"gnats" {:shape ["x", "y", "thyme"]}}}
                    "gnats")))))
  (with-private-fns [io.mandoline.dataset [validate-var-fill]]
    (testing "validate-var-fill"
      (let [valid? (fn [p c v] (nil? (validate-var-fill p c v)))
            invalid? (fn [p c v] (try (not (nil? (validate-var-fill p c v)))
                                     (catch IllegalArgumentException e
                                       true)))]
        (is (valid? {:variables {"locusts" {:fill-value -999}}}
                    {:variables {"locusts" {:fill-value -999}}}
                    "locusts"))
        (is (valid? {:variables {"locusts" {:fill-value Double/NaN}}}
                    {:variables {"locusts" {:fill-value Double/NaN}}}
                    "locusts"))
        (is (valid? {:variables {"locusts" {:fill-value Float/NaN}}}
                    {:variables {"locusts" {:fill-value Float/NaN}}}
                    "locusts"))
        (is (valid? {:variables {"locusts" {:fill-value Float/NaN}}}
                    {:variables {"locusts" {:fill-value Double/NaN}}}
                    "locusts"))
        (is (invalid? {:variables {"frogs" {:fill-value -999}}}
                      {:variables {"frogs" {:fill-value 0}}}
                      "frogs"))
        ;; int vs float
        (is (invalid? {:variables {"darkness" {:fill-value -1.00}}}
                      {:variables {"darkness" {:fill-value -1}}}
                      "darkness"))
        ;; missing value
        (is (invalid? {:variables {"disease" {:fill-value 0}}}
                      {:variables {"disease" {}}}
                      "disease"))))))
