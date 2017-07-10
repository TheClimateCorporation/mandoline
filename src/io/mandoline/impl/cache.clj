(ns io.mandoline.impl.cache
  "This implements a caching index store and a caching chunk store.

   The caching index store attempts to speed up writes by keeping all
   but the final index state local. Because there is a lot of
   read-then-write-then-read behavior with the index, this potentially
   saves a tremendous amount of network I/O.

   The caching chunk store attempts to keep a small cache of
   heavily-used chunks in-memory. Because most physically-realistic
   datasets are highly-redundant, the bulk of the indices point to a
   tiny cluster of chunks. Maintaining a local cache of these chunks
   can save a tremendous amount of network I/O during reads, since
   most queries are dominated by requests for the same chunk."
  (:require
   [clojure.core.cache :as c]
   [clojure.tools.logging :as log]
   [io.mandoline.utils :as utils]
   [io.mandoline.impl.protocol :as proto])
  (:import
   [java.nio ByteBuffer]))

;; NOTE: CACHING FOR DISTRIBUTED WRITERS IS NOT SUPPORTED!
;;
;; This implementation of flush-index assumes that there is only one
;; machine performing a write. Using this store to perform
;; distributed writes will almost certainly result in corrupted
;; indices.
(deftype CachingIndex [index-cache next-store]
  proto/Index

  (target
    [_]
    (proto/target next-store))

  (chunk-at
    [_ coordinates]
    (let [delayed-read (delay (proto/chunk-at next-store coordinates))
          updated-cache (swap! index-cache
                               #(if (c/has? % coordinates)
                                  (c/hit % coordinates)
                                  (c/miss % coordinates @delayed-read)))]
      (c/lookup updated-cache coordinates)))

  (chunk-at
    [_ coordinates version-id]
    (let [delayed-read (delay (proto/chunk-at next-store coordinates version-id))
          updated-cache (swap! index-cache
                               #(if (c/has? % coordinates)
                                  (c/hit % coordinates)
                                  (c/miss % coordinates @delayed-read)))]
      (c/lookup updated-cache coordinates)))

  (write-index
    [_ coordinates old-hash new-hash]
    (let [state (swap! index-cache
                       #(let [synched? (= old-hash (get % coordinates))]
                          (if synched?
                            (assoc % coordinates new-hash)
                            %)))]
      (= new-hash (get state coordinates))))

  ;; TODO: All stores should implement bulk read/write semantics.

  (flush-index
    [_]
    ;; write the entire index to next-store
    (log/debug "flushing CachingIndex store")
    (let [write-fn (fn [[coords chunk]]
                     (proto/write-index next-store coords nil chunk))]
      (->> (seq @index-cache)
           (utils/npmap write-fn)
           (dorun)))
    (proto/flush-index next-store)))


(defn- mk-cache
  "Creates a least-used cache with a time-to-live policy.

  An LU cache was chosen because to aggressively minimize the amount
  of I/O.

  Chunks are only cached after a cache miss on read, not after writing."
  [cache-size]
  (c/lru-cache-factory {} :threshold cache-size))

(deftype CachingChunkStore [next-store chunk-cache cache-size]
  proto/ChunkStore

  (read-chunk
    [_ hash]
    ;; implement read-through cache
    (let [delayed-read (delay (proto/read-chunk next-store hash))
          updated-cache (swap! chunk-cache
                               #(if (c/has? % hash)
                                  (c/hit % hash)
                                  (c/miss % hash @delayed-read)))]
      (c/lookup updated-cache hash)))

  (write-chunk
    [_ hash ref-count bytes]
    (proto/write-chunk next-store hash ref-count (.slice ^ByteBuffer bytes)))

  (update-chunk-refs
    [_ hash delta]
    (proto/update-chunk-refs next-store hash delta)))

(defn mk-caching-chunk-store [next-store options]
  (let [{:keys [cache-size] :or {cache-size 1000}} options]
    (->CachingChunkStore
     next-store
     (atom (mk-cache cache-size))
     cache-size)))

(defn mk-caching-index [next-store options]
  (let [{:keys [cache-size] :or {cache-size 1000}} options]
    (->CachingIndex
      (atom (mk-cache cache-size))
     next-store)))
