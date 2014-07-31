(ns io.mandoline.test.token
  (:use clojure.test)
  (:require
    [clojure.java.io :as jio]
    [io.mandoline :as db]
    [io.mandoline.utils :as utils]
    [io.mandoline.impl :as impl]
    [io.mandoline.test.utils :as test-utils]))

(defn test-token-conversion
  "This test tries to convert a ds-writer into a token and finalize the version
  using the new ds-writer."
  [setup teardown]
  (test-utils/with-temp-db spec setup teardown
    (testing (str "Token Serialization with store spec: " spec)
      (let [metadata (-> (jio/resource "test-foobar.json")
                         slurp (utils/parse-metadata true))
            ; there was a bug de-tokenizing previous-version,
            ; so ensure that there is a first version
            v0 (-> (db/dataset-writer spec)
                   (db/on-last-version)
                   (db/add-version metadata)
                   (db/finish-version))
            dw (-> (db/dataset-writer spec)
                   (db/on-last-version)
                   (db/add-version metadata))
            token (db/dataset-writer->token dw)
            dw2 (db/token->dataset-writer spec token)]
        (is (not (nil? dw2)))
        (is (= (impl/last-version (:connection dw2)) (:parent-version dw2)))
        (is (pos? (db/finish-version dw2)))))))

