(ns io.mandoline.chunk
  (:require
   [clojure.math.combinatorics :as combo]
   [clojure.math.numeric-tower :as math]
   [io.mandoline.slice :as slice])
  (:import
   [org.apache.commons.codec.digest DigestUtils]
   [io.mandoline.slice Slice]
   [ucar.ma2 Array]))

(defn to-chunk-coordinate
  "From a slice and the chunk grid slice, get the chunk-coordinate
  for the chunks contained."
  [^Slice slice ^Slice chunk-grid-slice]
  (let [range-fn (fn [start stop step grid]
                   (->> (range start stop step)
                        (map #(/ % grid))
                        (map (comp long math/floor))
                        distinct))]
    (->> [(:start slice) (:stop slice) (:step slice) (:step chunk-grid-slice)]
         ;; generate the range along each dimension
         (apply map range-fn)
         ;; calculate all chunk coordinates
         (apply combo/cartesian-product)
         (map vec))))

(defn from-chunk-coordinate
  "Gets the slice for a chunk at the chunk coordinate, given the chunk grid slice."
  [^Slice chunk-grid-slice chunk-coordinate]
  (let [start (map * (:step chunk-grid-slice) chunk-coordinate)
        stop (->> (map inc chunk-coordinate)
                  (map * (:step chunk-grid-slice)))]
    (slice/mk-slice start stop)))

(defn- array-sha1 [^Array a]
  "The array is hashed using the SHA-1 algorithm."
  (->> a
       (.getDataAsByteBuffer)
       (.array)
       (DigestUtils/shaHex)))

(defn generate-id
  "Generate a unique ID for a chunk based on its contents."
  [chunk]
  (-> chunk (:data) (array-sha1)))
