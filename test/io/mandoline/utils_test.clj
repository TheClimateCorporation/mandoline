(ns io.mandoline.utils-test
  (:use
   [clojure test])
  (:require
   [clojure.string :as string]
   [me.raynes.fs :as fs]
   [metrics.utils :refer (all-metrics)]
   [io.mandoline.utils :as utils]))

(defn- foo [a b] (+ a b))
(defn- bar [a b] (-> (foo a b) inc))
(defn- baz [a b] (* a b))

(deftest instrument-test
  (let [foo-time "io.mandoline.utils-test.foo.time"
        baz-time "io.mandoline.utils-test.baz.time"
        _ (utils/instrument [foo baz])]
    (is (= 0 (.getCount ((all-metrics) foo-time))))
    (is (= 0 (.getCount ((all-metrics) baz-time))))
    (is (foo 3 4))
    (is (= 1 (.getCount ((all-metrics) foo-time))))
    (is (bar 3 4))
    (is (= 2 (.getCount ((all-metrics) foo-time))))
    (is (baz 3 4))
    (is (= 1 (.getCount ((all-metrics) baz-time))))))

(deftest test-mk-temp-dir
  (testing "mk-temp-dir"
    (let [tmp-dir (utils/mk-temp-dir)]
      (is (not (string/blank? (.getPath tmp-dir)))) ; non-empty string path
      (is (.isDirectory tmp-dir)) ; is actually a directory
      (is (.canRead tmp-dir))
      (is (.canWrite tmp-dir)))))

(deftest test-with-deleting
  (testing "with-deleting"
    (let [tmp-atom (atom {})]
      (utils/with-deleting [tmp (utils/mk-temp-dir)]
        (is (.isDirectory tmp))
        (is (.canRead tmp))
        (is (.canWrite tmp))
        (let [foo (fs/file tmp "foo.txt")]
          (fs/touch (.getPath foo))
          (is (.exists foo))
          (is (.isFile foo))
          (reset! tmp-atom {:dir tmp :file foo})))
      (is (not (.exists (:dir @tmp-atom))))
      (is (not (.exists (:file @tmp-atom)))))))

(deftest test-with-deleting-exception
  (testing "with-deleting throwing an exception"
    (let [tmp-atom (atom {})]
      (is (thrown? RuntimeException
                   (utils/with-deleting [tmp (utils/mk-temp-dir)]
                     (is (.isDirectory tmp))
                     (reset! tmp-atom {:dir tmp})
                     (throw (RuntimeException. "barf!")))))
      (is (not (.exists (:dir @tmp-atom)))))))
