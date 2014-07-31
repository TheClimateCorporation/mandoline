(ns io.mandoline.filters.chain
  "CHUNK FORMAT after chain filter is applied
  HeaderVersion,??

  HeaderVersion (Byte)
  the first byte identifies the header version,
   right now only version 0 exists.

  VERSION 0
  0,FilterCount,Filter#0,Filter#1,Filter#N,Data..

  FilterCount (Byte)
  0x00 to 0xFF -> number of filters in the chain

  Filter#XX (Byte)
  0x00 to 0xFF -> filter code

  Data [Bytes]
  chunk data after the all filters in the chain were applied"
  (:require
    [io.mandoline.filters.lz4 :as lz4]
    [io.mandoline.filters.deflate :as deflate])
  (:import
    [java.nio ByteBuffer]))

(def ^:private filter-name-code
  "Maps a filter name to a filter code.
   NOTE: This should remain fixed to the lifetime of the database!"
  {"lz4" 1
   "lz4hc" 2
   "deflate" 3})

(def ^:private filter-code-applyfn
  "Maps a filter code to a corresponding apply function.
  Refer to filter-name-code for the code assignments!"
  {1 lz4/filter-apply
   2 lz4/filter-apply-hc
   3 deflate/filter-apply})

(def ^:private filter-code-reversefn
  "Maps a filter code to a corresponding reverse function.
  Refer to filter-name-code for the code assignments!"
  {1 lz4/filter-reverse
   2 lz4/filter-reverse
   3 deflate/filter-reverse})

(def ^:private default-filters [])

(defn- choose-filter
  "Choose a filter provider based on a filter name"
  [filter-name]
  (or (filter-name-code filter-name)
      (throw (IllegalArgumentException.
               (format "%s isn't a known filter" filter-name)))))

;; NOTE: the ByteBuffer passed to this function must have an accessible array
;;       the underlining array is never modified but may be used to avoid unnecessary
;;       copies along the chain.
(defn get-chain-apply
  "Parse the given dataset metadata and return a chain-apply function"
  [metadata var-name]
  (let [var-metadata (merge {:filters (:filters metadata default-filters)}
                            (-> metadata :variables var-name))
        filter-codes (map choose-filter (:filters var-metadata))
        filter-count (count filter-codes)
        filter-applyfns (map filter-code-applyfn filter-codes)]
    (fn [^ByteBuffer chunk-raw]
      (when chunk-raw
        (assert (.hasArray chunk-raw))
        (let [^ByteBuffer chunk-filtered (reduce #(%2 var-metadata %1)
                                                 chunk-raw
                                                 filter-applyfns)
              chunk-assembled (ByteBuffer/allocate (+ 2
                                                      filter-count
                                                      (.remaining chunk-filtered)))]
          (-> chunk-assembled
              (.put (byte 0))                             ;; version 0
              (.put (byte filter-count))                  ;; filter count
              (.put (byte-array (map byte filter-codes))) ;; filter codes
              (.put (.array chunk-filtered)               ;; filtered data
                    (+ (.arrayOffset chunk-filtered)
                       (.position chunk-filtered))
                    (.remaining chunk-filtered))
              (.flip)))))))

(defn get-chain-reverse
  "Return a chain-reverse function"
  []
  (fn [^ByteBuffer chunk-buffer]
    (when chunk-buffer
      (assert (.hasArray chunk-buffer))
      (assert (== 0 (.get chunk-buffer)))                     ;; version 0 only
      (let [filter-count (.get chunk-buffer)                  ;; filter count
            filter-codes (byte-array filter-count)
            chunk-buffer (.get chunk-buffer filter-codes)     ;; filter codes
            filter-reversefns (map filter-code-reversefn filter-codes)]
        (reduce #(%2 %1) chunk-buffer (reverse filter-reversefns))))))
