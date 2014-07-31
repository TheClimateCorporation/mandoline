(ns io.mandoline.slice
  (:require
    [clojure.math.combinatorics :as combo]
    [clojure.math.numeric-tower :as math]))

(defrecord Slice [start stop step])

(defn ^Slice mk-slice [start & more]
  "Constructor function for a Slice.

  Default value for stop is the (map inc start).
  Default value for step is (repeat (count start) 1).
  Step cannot be specified explicitly without stop being explicitly specified.

  ns=> (mk-slice [0 0 0])
  ns.Slice{:start [0 0 0], :stop [1 1 1], :step [1 1 1]}

  ns=> (mk-slice [0 0 0] [2 2 2])
  ns.Slice{:start [0 0 0], :stop [2 2 2], :step [1 1 1]}

  ns=> (mk-slice [0 0 0] [4 5 2] [2 2 2])
  ns.Slice{:start [0 0 0], :stop [4 5 2], :step [2 2 2]}"
  (let [[stop step] more
        stop (or stop (vec (map inc start)))
        step (or step (vec (repeat (count start) 1)))]
    (if (= (count stop) (count step) (count start))
      (->Slice start stop step)
      (throw
        (IllegalArgumentException.
          (format
            "Start, stop and step do not all have the same parity (%s %s %s)"
            start stop step))))))

(defn start<stop? [^Slice slice]
  (every? true? (map < (:start slice) (:stop slice))))

(defn get-shape [^Slice slice]
  {:pre [(start<stop? slice)]}
  (->> (map range (:start slice) (:stop slice) (:step slice))
       (map count)
       vec))

(defn- step-is-one? [^Slice slice]
  (every? #(= 1 %) (:step slice)))

(defn contains
  "Returns non-nil if slices are in monotonically non-decreasing order,
  and monotonically non-increasing size, otherwise false.

  b is the 'bigger' slice.  this doesn't make sense semantically, so it
  is a possible TODO to change this."
  ([a] true)
  ([a b]
     (if (empty? (:start b))
       (empty? (:start a))
       (and (every? true? (map >= (:start a) (:start b)))
            (every? true? (map <= (:stop a) (:stop b))))))
  ([a b & more]
     (and (contains a b) (apply contains more))))

(defn intersect-sorted-vector
  "Taken from:
  http://stackoverflow.com/questions/14160830/idiomatic-efficient/
  -clojure-way-to-intersect-two-a-priori-sorted-vectors"
  [x y]
  (loop [x x, y y, r (transient [])]
    (if (and (seq x) (seq y))
      (let [xi (first x), yj (first y)]
        (cond
          (< xi yj) (recur (next x) y r)
          (> xi yj) (recur x (next y) r)
          :else (recur (next x) (next y) (conj! r xi))))
      (persistent! r))))

;; DEPRECATED
;; This function, while more correct than get-intersection (it
;; correctly intersects striding slices) is a performance bottleneck.
;; Once we have a spare moment, we need to optimize the
;; stride-preserving logic in here and incorporate it into the faster
;; get-intersection function.
(defn ^Slice get-intersection-slow
  "Given two Slices, we return the intersection.

  The slices must be of the same parity."
  [^Slice a ^Slice b]
  {:pre [(apply = (map #(count (:start %)) [a b]))]}
  (if (= a b)
    a
    (let [get-range (fn [x]
                      (map range (:start x) (:stop x) (:step x)))
          intersection (->> (map get-range [a b])
                            (apply map intersect-sorted-vector))]
      (cond
        (not-any? #(= [] %) intersection)
        (let [[start stop] (->> (map #(apply (juxt min max) %) intersection)
                                (apply map vector))
              stop (map inc stop) ;; end is noninclusive
              step (map math/lcm (:step a) (:step b))]
          (mk-slice start stop step))
        :else (throw
                (IllegalArgumentException.
                  (format "The slices do not intersect: %s %s" a b)))))))

(defn ^Slice get-intersection
  [^Slice a ^Slice b]
  ;; Ensure that all slices have equal dimensionality and step=1 along
  ;; each dimension. This does not use the step-is-one? function
  ;; because it adds substantial computational overhead and this code
  ;; is a hot spot.
  {:pre [(let [a_step (:step a)
               b_step (:step b)]
           (and
            (= (repeat (count a_step) 1) a_step)
            (= a_step b_step)))]}
  (if (= a b)
    a
    (let [start (map max (:start a) (:start b))
          stop (map min (:stop a) (:stop b))
          step (map math/lcm (:step a) (:step b))
          extent (map - stop start)]
      ;; an intersection must have positive area in each dimension
      (if (every? #(> % 0) extent)
        (mk-slice start stop step)
        (throw
         (IllegalArgumentException.
          (format "The slices do not intersect: %s %s" a b)))))))

(defn ^Slice translate
  "Translates the coordinates to the new origin."
  [^Slice new-origin ^Slice slice]
  {:pre [(apply = (map #(count (:start %)) [new-origin slice]))]}
  (let [start (map - (:start slice) (:start new-origin))
        stop (map - (:stop slice) (:start new-origin))]
    (mk-slice start stop (:step slice))))

(defn iter
  "Iterates through the coordinates in the slice, with the last dimension
  iterating the fastest."
  [^Slice slice]
  (->> [(:start slice) (:stop slice) (:step slice)]
       (apply map range)
       (apply combo/cartesian-product)))
