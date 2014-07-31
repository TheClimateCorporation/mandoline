(ns io.mandoline.test.utils
  (:require
    [clojure.tools.logging :as log]
    [io.mandoline :as db]
    [io.mandoline.backend.mem :as mem]
    [io.mandoline
     [impl :as impl]
     [slab :as slab]
     [slice :as slice]
     [utils :as utils]]
    [io.mandoline.impl.protocol :as proto])
  (:import
    [java.util UUID]
    [java.io ByteArrayInputStream]
    [ucar.ma2 Array DataType]))

(defn random-name
  "Generate a random string."
  []
  (str (UUID/randomUUID)))

(defn setup-mem-spec
  "Create a random dataset spec map for testing the in-memory Mandoline
  backend.

  This function is intended to be used with the matching
  teardown-mem-spec function."
  []
  (let [root (format "test.%s" (random-name))
        dataset (random-name)]
    {:store "io.mandoline.backend.mem/mk-schema"
     :root root
     :dataset dataset}))

(defn teardown-mem-spec
  "Given a dataset spec map for the in-memory Mandoline backend,
  destructively clean up the test data and return nil.

  WARNING: This teardown function will not destroy schema data for an
  already instantiated MemSchema instances. Existing instances will
  continue to see the \"destroyed\" schema data, while new instances
  will not see this data after teardown.

  This function is intended to be used with the matching setup-mem-spec
  function."
  [spec]
  (mem/destroy-schema (:root spec))
  nil)

(defmacro with-temp-store
  "Create a temporary Mandoline database from 'uri', execute 'body',
  then clean up."
  [store-spec & body]
  `(let [s# (impl/mk-schema ~store-spec)]
     (try
       (db/create ~store-spec)
       ~@body
       (finally (proto/destroy-dataset s# (:dataset ~store-spec))))))

(defmacro with-temp-db
  "Create a temporary store spec and bind the symbol given by
  `store-spec` to this store spec, then execute `body` with this bound
  symbol.

  `setup` must be a zero-argument function that returns a store spec.
  The minimal valid store spec is a map that contains a `:schema` key
  whose value satisfies the Schema protocol.

  `teardown` must be a one-argument function that takes the return
  value of `setup` as its argument.

  Users of this macro are responsible for providing well-behaved `setup`
  and `teardown` functions that do not persist side effects outside of
  the context of this macro."
  [store-spec setup teardown & body]
  `(let [~store-spec (do
                       (log/debug "Calling setup function to create spec")
                       (~setup))]
     (try
       (log/debug "Using spec:" ~store-spec)
       (with-temp-store ~store-spec
         ~@body)
       (finally
         (log/debug "Calling teardown function on spec:" ~store-spec)
         (~teardown ~store-spec)))))

(defn bais->vec
  "Convert a ByteArrayInputStream to a vector of byte values."
  [^ByteArrayInputStream bais]
  (let [buff (byte-array (.available bais))]
    (.read bais buff)
    (vec buff)))

(defn get-shape [slab]
  (-> (:data slab)
      (.getShape)
      vec))

(defn get-underlying-array [slab]
  (-> (:data slab)
      (.copyTo1DJavaArray)
      vec))

(defn get-data-type [slab]
  (-> (:data slab)
      (.getElementType)
      (slab/as-data-type)))

(defn array=
  "Equality for arrays, and can handle nans."
  ([x] true)
  ([x y]
     (every? identity (map utils/nan= x y)))
  ([x y & more]
     (if (array= x y)
       (if (next more)
         (recur y (first more) (next more))
         (array= y (first more)))
       false)))

(defn same-as [expected-slab]
  (fn [actual]
    (and (= (:slice actual) (:slice expected-slab))
         (= (get-shape actual) (get-shape expected-slab))
         (array= (get-underlying-array actual) (get-underlying-array expected-slab)))))

(defn- to-dtype [array dtype]
  (case dtype
    "byte" (byte-array (map byte array))
    "char" (char-array (map char array))
    "double" (double-array array)
    "float" (float-array array)
    "int" (int-array array)
    "long" (long-array array)
    "short" (short-array (map short array))))

(defn- array-factory [array shape dtype]
  (->> (to-dtype array dtype)
       (Array/factory (DataType/getType dtype) (int-array shape))))

(defn random-slab
  "Given a list of dimension lengths and a ceiling, generates a slab with randomly
  generated data."
  [dtype slice ceiling]
  (let [shape (slice/get-shape slice)
        data (-> (repeatedly (apply * shape) #(rand ceiling))
                 (array-factory shape dtype))]
    (slab/->Slab data slice)))

(defn same-slab
  "Given a shape and a fill value, generates a slab of the given shape and fill value."
  [dtype slice fill]
  (let [shape (slice/get-shape slice)
        data (-> (apply * shape)
                 (repeat fill)
                 (array-factory shape dtype))]
    (slab/->Slab data slice)))

(defn to-slab
  [dtype slice array]
  (let [shape (slice/get-shape slice)]
    (-> (flatten array)
        (array-factory shape dtype)
        (slab/->Slab slice))))

(defmacro with-and-without-caches [& body]
  `(do ~@body
       (binding [io.mandoline.impl/use-cache? false]
         ~@body)))
