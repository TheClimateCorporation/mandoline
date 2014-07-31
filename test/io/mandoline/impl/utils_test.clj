(ns io.mandoline.impl.utils-test
  (:use [clojure test])
  (:require [io.mandoline.impl :as impl]))

(deftest test-uri->store-spec
  (is (= {:store "file" :root "/tmp/foo" :db-version nil :dataset "mydataset"}
         (impl/uri->store-spec "file:///tmp/foo/mydataset")))

  (is (= {:store "ddb" :root "foo.com" :db-version nil :dataset "mydataset"}
         (impl/uri->store-spec "ddb://foo.com/mydataset")))

  (with-redefs [impl/db-version "1"]
    (is (= {:store "file" :root "/tmp/foo" :db-version "1" :dataset "mydataset"}
           (impl/uri->store-spec "file:///tmp/foo/mydataset")))

    (is (= {:store "ddb" :root "foo.com" :db-version "1" :dataset "mydataset"}
           (impl/uri->store-spec "ddb://foo.com/mydataset")))))
