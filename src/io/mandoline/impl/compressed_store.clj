(ns io.mandoline.impl.compressed-store
  (:require
   [clojure.tools.logging :as log]
   [io.mandoline.filters.chain :as chain]
   [io.mandoline.impl.protocol :as proto]))

(def ^:private chain-apply (chain/get-chain-apply {:filters ["lz4"]} :na))

(def ^:private chain-reverse (chain/get-chain-reverse))

(deftype ChainChunkStore [next-store]
  proto/ChunkStore

  (read-chunk
    [_ hash]
    (-> (proto/read-chunk next-store hash)
        chain-reverse))

  (write-chunk
    [_ hash ref-count bytes]
    (->> bytes
         chain-apply
         (proto/write-chunk next-store hash ref-count)))

  (update-chunk-refs
    [_ hash delta]
    (proto/update-chunk-refs next-store hash delta)))

(defn mk-compressed-store [next-store options]
  (->ChainChunkStore next-store))

