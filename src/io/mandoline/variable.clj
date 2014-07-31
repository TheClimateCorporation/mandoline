(ns io.mandoline.variable
  (:require
   [io.mandoline.chunk :as chunk]
   [io.mandoline.slab :as slab]
   [io.mandoline.slice :as slice])
  (:import
   [ucar.ma2 DataType Array]
   [io.mandoline.slice Slice]))

(defn update
  "Update an existing variable definition."
  [token var-name def]
  (assoc-in token [:variables var-name] def))

(defn ^String get-type
  "Look up the type string for a given variable. Type strings are
  ucar.ma2.DataType-compatible strings."
  [token var-name]
  (get-in token [:variables var-name :type]))

;; TODO: Put this elsewhere, as it's an implementation detail.
(defn ^ucar.ma2.DataType get-dtype
  "Return the ucar.ma2.DataType for a given variable."
  [token var-name]
  (-> (get-type token var-name)
      (slab/as-data-type)))

(defn get-fill
  "Look up the default value for this array if none is specified."
  [token var-name]
  (get-in token [:variables var-name :fill-value]))

(defn get-variable-dimensions
  [token var-name]
  (let [shape (get-in token [:variables var-name :shape])]
    (assert shape (format "No shape detected for variable %s" var-name))
    ;; the corresponding keys are keywords
    (map keyword shape)))

(defn ^io.mandoline.slice.Slice get-var-slice
  "Look up the shape of a given variable on the original grid."
  [token var-name]
  (let [dimensions (get-variable-dimensions token var-name)
        start (repeat (count dimensions) 0)
        stop (map #(get (token :dimensions) %) dimensions)]
    (slice/mk-slice start stop)))

(defn ^io.mandoline.slice.Slice get-chunk-grid-slice
  "Look up the shape of a given variable on the chunk grid."
  [token var-name]
  (let [dimensions (get-variable-dimensions token var-name)
        start (repeat (count dimensions) 0)
        stop (map #(get (token :dimensions) %) dimensions)
        chunk-size (map #(get (token :chunk-dimensions) %) dimensions)]
    (slice/mk-slice start stop chunk-size)))

(defn ^io.mandoline.slice.Slice get-chunk-slice
  "Look up the chunk slice for a given variable.

  Returns the slice of the specific chunk at given chunk-coordinate; it will
  be no larger than the dimensions defined in the dataset definition. Throws
  an IllegalArgumentException if the chunk at chunk-coordinate does not overlap
  the dataset at all.

  THIS WILL NOT BE CROPPED SMALLER AS TO NOT EXTEND BEYOND THE DATASET."
  [token var-name chunk-coordinate]
  (let [var-slice (get-var-slice token var-name)
        ;; convert chunk-coordinate to data coordinates
        chunk-slice (-> (get-chunk-grid-slice token var-name)
                        (chunk/from-chunk-coordinate chunk-coordinate))]
    ;; check (:stop chunk-slice) is >= (:stop var-slice)
    (assert (every? >= (map vector (:stop chunk-slice) (:stop var-slice))))
    chunk-slice))

(defn get-index
  "Retrieve the index ID for var-name from token."
  [token var-name]
  (get-in token [:variables var-name :index]))

(defn set-index
  "Set the index ID for var-name in token."
  [token var-name index]
  (assoc-in token [:variables var-name :index] index))
