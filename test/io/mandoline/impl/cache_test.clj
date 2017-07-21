(ns io.mandoline.impl.cache-test
  (:require
    [clojure.test :refer :all]
    [io.mandoline.backend.mem :as mem]
    [io.mandoline.impl.protocol :as proto]
    [io.mandoline.impl.cache :as cache]
    [io.mandoline.test.utils :as utils]))

(deftest unit-test
    (let [sch (mem/mk-schema :foo)
          name (utils/random-name)
          _ (proto/create-dataset sch name)
          conn (proto/connect sch name)
          idx (proto/index conn :myvar {:version-id 1234} {})
          cache-idx (cache/mk-caching-index idx {})]

      ; Write to the backend directly
      (is (proto/write-index idx [1 2 3] nil "myhash1"))
      ; Test `read` with and without cache
      (is (= "myhash1" (proto/chunk-at cache-idx [1 2 3])))
      (is (= "myhash1" (proto/chunk-at idx [1 2 3])))

      (proto/destroy-dataset sch name)))
