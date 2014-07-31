(ns io.mandoline.test.namespaces_test
  "This project provides test tools for other projects. Initially the
  only testing we provide for the test tools is the fact that all of its
  namespaces can be created."
  (:require
    [clojure.test :refer :all]
    [clojure.java.io :refer [file]]
    [clojure.tools.namespace :refer [find-namespaces-in-dir]]))

(deftest load-all
  (doseq [ns (find-namespaces-in-dir (file "src"))]
    (require ns))
  (is :ok))
