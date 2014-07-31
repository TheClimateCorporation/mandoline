(ns io.mandoline
  "\"Porcelain\" (versus plumbing) functions for Mandoline.

  Clients wanting to use mandoline library can use the
  `io.mandoline` namespace, both for reading and writing.

  The design of the library is guided by the need to keep information
  in memory to optimize both reads and writes. A dataset reader/writer
  can be kept in memory for as long as the client needs to read/write to
  a dataset. A variable reader/writer can be kept in memory for as long
  as the reads/writes apply to the *same version* of a dataset.

  To read a Mandoline dataset, the client does the following:

  - Specify the dataset with a Mandoline URI or a spec map.
  - Create a version-unspecified dataset reader by calling the function
    `io.mandoline/dataset-reader` with the spec map as the
    argument.
  - Created a *versioned* dataset reader by calling either
    `io.mandoline/on-nth-version` or
    `io.mandoline/on-timestamp` with the dataset reader as the
    argument.
  - Create a variable reader by calling the function
    `io.mandoline/variable-reader` with the dataset reader and
    the variable keyword as the two arguments.
  - Call the function `io.mandoline/get-slice` with the variable
    reader and a request slice as the two arguments. The request slice
    specifies the extent of the data to read from the variable. It can
    be constructed with the function
    `io.mandoline.slice/mk-slice`.

  To write a Mandoline dataset, the client does the following:

  - Specify the datset with a Mandoline URI or a spec map.
  - Create a version-unspecified dataset writer by calling the
    `io.mandoline/dataset-writer` function with the spec map as
    the argument.
  - Created a *versioned* dataset writer by calling the
    `io.mandoline/on-last-version` function with the dataset
    writer as the argument.
  - Created a *versioned* dataset writer for a *new* version by calling
    the `io.mandoline/add-version` function with the versioned
    dataset reader as the argument. You always need to create a new
    version before writing data to Mandoline.
  - Create a variable writer by calling the
    `io.mandoline/variable-writer` function with the dataset
    writer and the variable keyword as the two arguments.
  - Call the `io.mandoline/write` function with the variable
    writer and a seq of slabs as the two arguments. A slab is a record
    that contains a (possibly multidimensional) array and indices that
    specify where in the variable the array values are to be written.
    A slab can be constructed with the record factory function
    `io.mandoline.slab/->Slab`.
  - When all slabs are written, \"commit\" the changes by calling the
    function `io.mandoline/finish-version` with the dataset
    writer as the argument.

  For detailed usage examples, see the mandoline project README."
  (:require
   [clojure.tools.logging :as log]
   [io.mandoline.utils :as utils]
   [io.mandoline.chunk :as chunk]
   [io.mandoline.dataset :as dataset]
   [io.mandoline.variable :as variable]
   [io.mandoline.slab :as slab]
   [io.mandoline.slice :as slice]
   [io.mandoline.impl.protocol :as proto]
   [io.mandoline.impl :as impl])
  (:import
   [io.mandoline.slab Slab]))


(defn list-datasets [store-spec]
  (-> store-spec impl/mk-schema proto/list-datasets))

(defn versions [store-spec]
  "Given a store-spec, returns a seq of [<DateTime> <version-id>] tuples."
  (->> (-> store-spec impl/mk-connection (proto/versions {}))
       (map (juxt :timestamp :version))))

(defn last-version [store-spec]
  "Given a store-spec, returns the last version-id."
  (-> store-spec impl/mk-connection impl/last-version))

(defn metadata [store-spec & [^String version]]
  "The version here is the version id, which is a string, obtained by
  a call to (versions)"
  (let [conn (impl/mk-connection store-spec)]
    (if version
      (proto/metadata conn version)
      (-> (proto/versions conn {:limit 1 :metadata? true}) first :metadata))))

(defn dataset-exists? [store-spec]
  "Check to see if the dataset exists."
  (-> store-spec list-datasets set (contains? (:dataset store-spec))))

(defn dataset-reader [store-spec]
  (impl/mk-store
    store-spec
    #(impl/wrap %1
                (or (-> %2 :dataset-spec :store :wrappers)
                    (impl/default-store-reader-wrappers %2))
                store-spec)))

(defn dataset-writer
  ([store-spec]
     (impl/mk-store
      store-spec
      #(impl/wrap %1
                  (or (-> %2 :dataset-spec :store :wrappers)
                      (impl/default-store-writer-wrappers %2))
                  store-spec)))
  ([store-spec ingest-opts]
     (let [writer (dataset-writer store-spec)]
       (assoc writer :ingest ingest-opts))))

(defn on-version [dataset version]
  (assoc dataset
         :version version
         :metadata (when version
                     (proto/metadata (:connection dataset) version))))

(defn nth-version [dataset n]
  (impl/nth-version (:connection dataset) n))

(defn timestamp->version [dataset ts]
  (impl/timestamp->version (:connection dataset) ts))

(defn on-nth-version
  "Setup the reader/writer to act on version indexed n. Versions start with the
  index 0 and negative n values index from the end. So to access the last
  version you would do (on-nth-version dataset -1)"
  [dataset n]
  (on-version dataset (nth-version dataset n)))

(defn on-timestamp [dataset timestamp]
  (on-version dataset
              (impl/timestamp->version (:connection dataset) timestamp)))

(defn on-last-version [dataset]
  (on-nth-version dataset -1))

(defn add-version
  [dataset-writer metadata]
  (let [connection (:connection dataset-writer)
        parent-version (:version dataset-writer)]
    (assert (= parent-version (impl/last-version connection))
            "parent version does not match the most recent")
    (dataset/validate-dataset-definition metadata)
    (let [metadata (-> (if parent-version
                         (dataset/inherit metadata (:metadata dataset-writer))
                         (dataset/create metadata))
                       dataset/new-version)]
      (assoc dataset-writer
             :metadata metadata
             :version (:version-id metadata)
             :parent-version parent-version
             :parent-metadata (:metadata dataset-writer)))))

(defn dataset-writer->token
  [dataset-writer]
  (:metadata dataset-writer))

(defn token->dataset-writer
  [store-spec token]
  ; FIXME version type is not standardized between long and string
  ; :parent-version is a string, but :parent from :metadata is a long
  (let [parent (:parent token)
        parent-version (when parent (str parent))
        parent-metadata (when parent-version
                          (metadata store-spec parent-version))]
    (-> (dataset-writer store-spec)
        (assoc :metadata token
               :version (:version-id token)
               :parent-version parent-version
               :parent-metadata parent-metadata))))

(defn variable-reader
  ([dataset-reader var-name] (variable-reader dataset-reader var-name {}))
  ([dataset-reader var-name options]
   (let [{:keys [connection metadata]} dataset-reader]
     (assoc dataset-reader
            :var-name var-name
            :index (impl/wrap
                     (proto/index connection var-name metadata options)
                     (or (:wrappers options)
                         (impl/default-index-reader-wrappers options))
                     options)))))

(defrecord VariableWriter [variable-writer]
  java.io.Closeable
  (close [_]
    (let [{:keys [index parent-index]} variable-writer]
      (when parent-index
        (proto/flush-index parent-index))
      (proto/flush-index index))))

(defn variable-writer
  "Creates a variable writer for writing data to.

  Clients must close the variable writer once they are done with it.  They
  can do it by calling (.close variable-writer), but the preferred mechanism
  is using clojure's with-open macro."
  ([dataset-writer var-name] (variable-writer dataset-writer var-name {}))
  ([dataset-writer var-name options]
   (let [{:keys [connection metadata parent-metadata]} dataset-writer
         var-writer (assoc dataset-writer
                      :index (impl/wrap
                              (proto/index connection var-name metadata options)
                              (or (:wrappers options)
                                  (impl/default-index-writer-wrappers options))
                              options)
                      :parent-index
                      (when parent-metadata
                        (impl/wrap
                         (proto/index connection var-name parent-metadata options)
                         (or (:parent-wrappers options)
                             (impl/default-parent-index-writer-wrappers options))
                         options)))]
     (->VariableWriter var-writer))))


(defn stream
  "Returns a stream of slabs of the data in the variable within the
  bounds of 'request-slice'. Subsets the chunks so that only the data
  in the request is streamed over. Bounds checking is performed by
  default."
  ([variable-reader request-slice check-bounds?]
   (let [{:keys [metadata var-name index chunk-store]} variable-reader
         dtype (variable/get-dtype metadata var-name)
         var-slice (variable/get-var-slice metadata var-name)
         read-fn  (fn [coordinate]
                    (let [chunk-slice (variable/get-chunk-slice metadata
                                                                var-name
                                                                coordinate)
                          hash (impl/chunk-at index coordinate)]
                      (-> (if hash
                            (impl/hash->slab hash chunk-store dtype chunk-slice)
                            (impl/blank-slab metadata var-name dtype chunk-slice))
                          (slab/intersect request-slice))))]
     ;; FIX: One unfortunate side-effect of disabling bounds-checking
     ;; is that, if a variable is shrunk and the client doesn't know
     ;; it, they could retrieve "old" data from a parent index.
     (when (and check-bounds?
                (not (slice/contains request-slice var-slice)))
       (throw (IndexOutOfBoundsException.
               (format "Request slice %s crosses the variable's extent: %s."
                       (prn-str request-slice)
                       (prn-str var-slice)))))
     (->>
      (variable/get-chunk-grid-slice metadata var-name)
      ;; calculate chunk coordinates that span the requested slab
      (chunk/to-chunk-coordinate request-slice)
      ;; read all the chunks in the span
      ;; todo make number of threads configurable
      (utils/npmap read-fn))))
  ([variable-reader request-slice]
   (stream variable-reader request-slice true)))

(defn ^io.mandoline.slab.Slab get-slice
  "Retrieve a subsection of the data in the variable"
  ([variable-reader request-slice check-bounds?]
   (let [{:keys [metadata var-name]} variable-reader
         dtype (variable/get-dtype metadata var-name)
         response (->> (variable/get-fill metadata var-name)
                       (slab/empty dtype request-slice))]
     (->> (stream variable-reader request-slice check-bounds?)
          (reduce slab/merge response))))
  ([variable-reader request-slice]
   (get-slice variable-reader request-slice true)))

;;; Write ;;

(defn create
  "Create a new dataset with spec"
  [store-spec]
  (let [spec (impl/mk-store-spec store-spec)]
    (proto/create-dataset (impl/mk-schema spec) (:dataset spec))))

(defn write
  "slabs is a lazy seq of Slabs"
  [variable-writer slabs]
  (let [{:keys [chunk-store index parent-index]} (:variable-writer variable-writer)]
    (try
      (impl/write-variable chunk-store index parent-index slabs)
      (catch java.lang.InterruptedException e
        (log/error "Write canceled:" (pr-str variable-writer)))
      (catch Exception e
        (log/errorf e "Error writing to variable: %s" (pr-str variable-writer))
        (throw e)))))

(defn finish-version
  "Assign a version to the dataset and make it available for querying.
  Finalized datasets are immutable, so calling write subsequent to
  calling finish will fail. Returns the version string"
  [dataset-writer]
  ;; TODO: this is an imperfect fix, since it is not in a single transaction
  ;; Need to change the schema for this to work perfectly, e.g.
  ;; we should make 'parent-version' part of the key" or something
  (let [{:keys [connection metadata chunk-store version parent-version]}
        dataset-writer]
    (if (= (impl/last-version connection) parent-version)
      ;; This is not atomic because we're not checking that the last version
      ;; is the expected parent in a single transaction.
      ;; This has a critical region of only a few milliseconds.
      (proto/write-version connection metadata)
      (throw (Exception.
              (format "Dataset already exists with parent version %s."
                      parent-version))))
    version))

(defn instrument! []
  (utils/instrument [impl/last-version
                     impl/chunk-at
                     impl/hash->slab
                     impl/blank-slab
                     versions
                     metadata
                     list-datasets
                     stream
                     get-slice]))
