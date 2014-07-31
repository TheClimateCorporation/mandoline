(ns io.mandoline.slab
  (:require
    [io.mandoline.slice :as slice])
  (:import
    [io.mandoline.slice Slice]
    [java.util Arrays]
    [ucar.ma2 Array DataType MAMath])
  (:refer-clojure :exclude [empty merge]))

;; A Slab is an n-dimensional array paired with its coordinates.
;; Chunks are persistent instances of Slabs.
(defrecord Slab [^Array data ^Slice slice])

(defn- as-primitive-type
  "Return the primitive class type (e.g. Short/TYPE) of the given
  ucar.ma2.DataType."
  [^DataType datatype]
  (.getPrimitiveClassType datatype))

(defn- cast-to
  "Returns a primitive that is 'value' casted to the given
  primitive-class."
  [primitive-class value]
  (condp = primitive-class
    Byte/TYPE      (byte value)
    Character/TYPE (char value)
    Short/TYPE     (short value)
    Integer/TYPE   (int value)
    Long/TYPE      (long value)
    Float/TYPE     (float value)
    Double/TYPE    (double value)
    (throw (IllegalArgumentException.
            "primitive-class is not a primitive class"))))

(defn ^ucar.ma2.DataType as-data-type
  "Return the ucar.ma2.DataType enum value corresponding with the given
  name or Java Class"
  [string-or-class]
  (DataType/getType string-or-class))

(defn- ^ucar.ma2.DataType get-data-type
  [^Slab slab]
  (-> (:data slab)
      (.getElementType)
      as-data-type))

(defn ^Slab empty
  "Returns a new slab of 'type' and 'slice', initialized to 'fill'.
  'type' must be a valid primitive class instance (e.g. Short/TYPE).

   If a single value 'fill' is not specified, the Array's values are
   initialized to Java's default values for the datatype
   (docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html)
      0      for int/long/short/byte
      0.0    for double/float
      \u0000 for char
      false  for boolean
      nil    for String and any other object."
  [^DataType type ^Slice s & [fill]]
  (let [shape (int-array (slice/get-shape s))
        backing (->> shape
                     (apply *)
                     (make-array (as-primitive-type type)))]
    (when fill
      (->> (cast-to (as-primitive-type type) fill)
           (Arrays/fill backing))) ; execute for side-effects
    (-> (Array/factory type shape backing)
        (->Slab s))))

(defn ^Slab subset
  "Subsets the slab with a memory efficient view of the array.  The stride
  for the view slice must either be the same as that of the array-slice,
  or slab-slice step must all be ones."
  [^Slab slab ^Slice view]
  {:pre [(slice/contains view (:slice slab))
         (or (= (get-in slab [:slice :step]) (:step view))
             (every? #(= 1 %) (get-in slab [:slice :step])))]}
  (let [origin (int-array (map - (:start view) (get-in slab [:slice :start])))
        shape (int-array (slice/get-shape view))
        stride (if (= (get-in slab [:slice :step]) (:step view))
                 (int-array (repeat (count origin) 1))
                 (int-array (:step view)))]
    (-> (.sectionNoReduce (:data slab) origin shape stride)
        (->Slab view))))

(defn ^Slab intersect
  "Get an intersection of the slab with the slice.  The slab does not
  necessarily have to contain the slice.
  Stride requirements are the same as subset."
  [^Slab slab ^Slice view]
  (let [intersection (slice/get-intersection (:slice slab) view)]
    (if (= intersection (:slice slab))
      slab
      (subset slab intersection))))

(defn ^Slab copy-into
  "Copies the entire src array over the cells of dst that correspond to
  the src.

  Thus the src MUST fit in dst without surpassing
  dst's boundaries; otherwise an Assertion Exception is thrown.

  Mutates the dst reference and returns it."
  [^Slab src ^Slab dst]
  {:pre [(= (get-data-type src) (get-data-type dst))
            (slice/contains (:slice src) (:slice dst))]}
  (let [data-type (get-data-type src)
        src-iter (.getIndexIterator (:data src))
        dst-iter (-> (subset dst (:slice src))
                     (get :data)
                     (.getIndexIterator))]
    (MAMath/copy data-type src-iter dst-iter) ; execute for side-effects
    dst))

(defn ^Slab merge
  "Returns a slab representing the contents of 'src' merged on top of
  the contents of 'dst', wherever the two intersect. This modifies 'dst'
  by overwriting the contents of dst where src and dst overlap."
  [^Slab dst ^Slab src]
  (-> (intersect src (:slice dst))
      (copy-into dst)))
