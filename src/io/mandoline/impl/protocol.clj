(ns io.mandoline.impl.protocol)

(defprotocol Index
  ;; TODO: Get rid of this. It only confuses the interface.
  (target [_]
   "returns {:version foo :metadata bar :var-name var}")

  (chunk-at  [_ coordinates] [_ coordinates version-id]
   "Returns the chunk hash at coordinates (a vector). This operation doesn't
   need to be consistent, but consistent will obviously improve performance.
   If there is no chunk at coordinates, nil is returned. If version-id is given
   only chunks for that version will be returned, version-id can be an
   uncommitted version")

  (write-index [_ coordinates old-hash new-hash]
   "Writes the index at coordinates pointing to the chunk with hash new-hash.
   The write is processed only of the previous hash was old-hash. This is an
   atomic operation. Returns true if new-hash was written")

  (flush-index [_]
   "Write to persistent storage"))

(defprotocol ChunkStore
  (read-chunk [_ hash]
   ;; fixme this needs to return both the bytes and the ref count
   "Returns a ByteBuffer with the contents of the chunk that the hash
   points to, or throws exception if the hash is not found. This
   operation must be consistent.")

  (chunk-refs [_ hash]
   "Returns the Long representing reference count of the chunk that the
   hash points to, or throws exception if the hash is not found.")

  (write-chunk [_ hash ref-count bytes]
   "Write the chunk given by bytes ByteBuffer, and identified by hash, with the
    given ref-count, and returns nil. An exception is thrown if the
    ByteBuffer has zero remaining bytes to read, or if the hash is
    empty.")

  (update-chunk-refs [_ hash delta]
   "Increments chunk hash ref count by delta and returns nil, or throw
   an exception if the hash is not found."))

(defprotocol Connection
  "Provides information referent to a given dataset and creates index and
  chunk-store instances"

  (index [_ var-name metadata options]
    "Returns an instance of Index to write to the corresponding var-name and
    metadata. metadata includes a :version-id keyword. var-name is a keyword
    and metadata a map.

    At this time there are not generic options. Implementation can accept custom
    options as keywords namespaced by the implementation namespace")

  (chunk-store [_ options]
   "Returns a ChunkStore to read and write blobs from the dataset. options
   is a possibly empty map with ChunkStore options. At this time there are no
   generic options, each implementation can take custom options. Custom option
   names must be keywords namespaced by the implementation namespace

   Example:

   (chunk-store connection {:io.mandoline.impl.fs/file.consistency-delay-ms
                            100
                            :io.mandoline.impl.fs/file.consistency-retries
                            5})")

  (write-version [_ metadata]
   ;; todo explain more, probably change the name. I don't like that we need
   ;; to pass the same metadata to (index) and (write-version), we need
   ;; something else
   "Given the metadata map used to write to a dataset, commits the version")

  (get-stats [_]
   "Return a map of statistics about the dataset

    Ex:

      {:index-size 123456
       :metadata-size 23456
       :data-size 8284915324}")

  (metadata [_ version]
   "Retrieves the metadata map for the given version. If the version doesn't
   exist, an exception is thrown. version must be a valid version-id string.
   metadata is a clojure map with the following structure:

   {:version-id string: the id for the version
    :id string: the id for the dataset
   .... todo ... list all of them}

   Extra custom keys can appear in the metadata")

  (versions [_ opts]
   "Retrieve a (potentially lazy) seq of
   {:timestamp <DateTime>
    :version <version-id>
    :metadata <metadata>]
   maps representing the available versions of the dataset. The collection is in
   reverse chronological order. DateTime is the joda DateTime type and
   version-id are strings. If no versions exist for the dataset the operation
   returns an empty seq.  Metadata is the clojure map of the metadata"))

(defprotocol Schema
  "Inspect and modify the mandoline schema"

  (create-dataset [_ name]
   "Creates a dataset of the given name. Throws exception if failed. When the
   call returns, the dataset is ready to be written to. Attempts to create an
   existing dataset will result in an exception")

  (destroy-dataset [_ name]
   "Destroys the dataset with the given name. If the dataset doesn't exists
   no errors are raised and the operation is a NoOp")

  (list-datasets [_]
   "Returns a (potentially lazy) seq of all dataset names, without any
   particular order")

  (connect [_ dataset-name]
   "Returns a Connection to the dataset of the given name. The dataset must
   exist or an exception will be thrown"))
