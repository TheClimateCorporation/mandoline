(ns io.mandoline.impl
  (:require
   [clojure.set :as cset]
   [clojure.string :as string]
   [clojure.tools.logging :as log]
   [io.mandoline.impl
    [cache :as cache]
    [compressed-store :as compressed-store]
    [protocol :as proto]]
   [io.mandoline
    [slab :as slab]
    [slice :as slice]
    [utils :as utils]
    [chunk :as chunk]
    [variable :as variable]])
  (:import
   [java.net URI]
   [org.joda.time DateTime]
   [java.nio ByteBuffer]
   [ucar.ma2 Array DataType]))

(defn- validate-spec [spec]
  (let [needs1 #{:store :root :dataset}
        needs2 #{:dataset :schema}]
    (if (or (every? spec needs1) (every? spec needs2))
      spec
      (throw (IllegalArgumentException.
               (format
                 "Invalid store-spec %s, it should have at least the keys: %s or %s"
                 spec (pr-str needs1) (pr-str needs2)))))))

(defonce ^:dynamic db-version nil)

(defn uri->store-spec [uri]
  (let [uri (-> (URI. uri) (.normalize))
        paths (string/split (string/replace (.getSchemeSpecificPart uri) #"^//" "") #"/")]
    ;; TODO: add .getQuery handler
    {:store (.getScheme uri)
     :db-version db-version
     :root (string/join "/" (butlast paths))
     :dataset (last paths)}))

(defn mk-store-spec [spec]
  (validate-spec
    (cond (map? spec) (assoc spec :db-version db-version)
          (string? spec) (uri->store-spec spec)
          (isa? URI (class spec)) (uri->store-spec spec)
          :else
          (throw (IllegalArgumentException.
                   (str  "Invalid store-spec type: "
                        (type spec)))))))

(defn nth-version
  "Get the version-id for the nth version of the dataset. For small, non-negative
  values of n, in datasets with many versions, this could be a slow operation.
  For negative n, it indexes from the last version, so to get the version-id
  for the last version: (nth-version connection -1). For values of n out of
  bounds it returns nil"
  [connection n]
  (if (< n 0)
    (let [n (* -1 n)
          versions (proto/versions connection {:limit n})]
      (when (<= n (count versions))
        (-> versions (nth (dec n)) :version)))
    (let [versions (proto/versions connection {})]
      (when (< n (count versions))
        (-> versions reverse (nth n) :version)))))

(defn last-version
  ([connection]
   (nth-version connection -1))
  ([connection timestamp]
   (-> (drop-while #(.isAfter (:timestamp %) timestamp)
                   (proto/versions connection {}))
       first :version)))

(def ^:private schema-aliases
  {"file" "io.mandoline.backend.sqlite/mk-schema"
   "ddb" "io.mandoline.backend.dynamodb/mk-schema"
   "mem" "io.mandoline.backend.mem/mk-schema"
   "s3" "io.mandoline.backend.s3/mk-schema"
   "hybrid-s3-dynamodb" "io.mandoline.backend.hybrid-s3-dynamodb/mk-schema"})

(defn- resolve-schema-alias
  "Returns a function var using the store alias. It expects store to be a var
  string (ns/name) or one of the aliases in schema-aliases.

  It throws IllegalArgumentException if it can't find a proper var or alias"
  [store]
  (when (or (not (string? store)) (string/blank? store))
    (throw (IllegalArgumentException.
             "mandoline spec must contain a schema or an schema alias")))
  (let [var-string (get schema-aliases store store)
        [ns var & rest] (string/split var-string #"/")]
    (when (or (not (nil? rest)) (string/blank? var) (string/blank? ns))
      (throw (IllegalArgumentException.
               "mandoline spec schema alias must include namespace and var")))
    (require (symbol ns))
    (ns-resolve (symbol ns) (symbol var))))

(defn mk-schema
  "Returns an instance of Schema. If store-spec has a :schema key the value is
  returned. Otherwise the :store key is used as an alias. :store can contain a
  proper var string in the format namespace/function-name pointing to a function
  that will take store-spec as an argument and return a Schema.

  The special aliases \"file\", \"ddb\", and \"mem\" are supported,
  pointing to the builtin backends"
  [store-spec]
  (let [result (or (:schema store-spec)
                   ((resolve-schema-alias (:store store-spec)) store-spec))]
    (when (not (satisfies? proto/Schema result))
      (throw (IllegalArgumentException.
              (str "Can't find a Schema instance using " (pr-str store-spec)))))
    result))

(defn mk-connection [store-spec]
  (let [spec (mk-store-spec store-spec)]
    (-> spec mk-schema (proto/connect (:dataset spec)))))

(defn timestamp->version [connection timestamp]
  (cond
    (integer? timestamp) (timestamp->version connection (DateTime. timestamp))
    (nil? timestamp) (last-version connection)
    :else (last-version connection timestamp)))

(def ^:dynamic use-cache? true)

;; Caching store should be the last store in the wraper list so it caches
;; the output of compressed-store and not the final store.
(defn default-store-reader-wrappers [& _]
  (if use-cache?
    [compressed-store/mk-compressed-store cache/mk-caching-chunk-store]
    [compressed-store/mk-compressed-store]))
(def default-index-reader-wrappers
  (constantly []))
(defn default-store-writer-wrappers [& _]
  (if use-cache?
    [compressed-store/mk-compressed-store cache/mk-caching-chunk-store]
    [compressed-store/mk-compressed-store]))
(defn default-index-writer-wrappers [& _]
  (if use-cache?
    ;;CachingIndex introduces concurrency problems when ingesting a variable
    ;;that spans multiple netCDF files, that touch the same chunk.
    ;;
    ;;This causes some variable writers not to see that a chunk has been
    ;;modified, try writing on a blank one, and then failing silently
    ;;when the variable writer flushes.  This causes missing data, with
    ;;the second chunk written losing, because there is no good retry
    ;;mechanism for it.  Thus index caching has been disabled.

    ;;There are no noticeable performance decreases due to this.
    [#_cache/mk-caching-index]
    []))
(def default-parent-index-writer-wrappers
  (constantly []))

(defn wrap
  [o wrappers options]
  (reduce #(%2 %1 options) o wrappers))

(defn mk-store [spec wrappers-fun]
  (let [spec (mk-store-spec spec)
        schema (or (:schema spec)  (mk-schema spec))
        connection (or (:connection spec) (proto/connect schema (:dataset spec)))
        res {:dataset-spec spec :schema schema
             :connection connection}]
    (assoc res
           :chunk-store
           (wrappers-fun (proto/chunk-store connection (-> spec :store)) res))))

(defn mk-index [dataset var-name options wrappers-fun]
  {:pre [(every? dataset #{:connection :version})]}
  (let [connection (:connection dataset)
        version (:version dataset)
        mdata (or (:metadata dataset) (proto/metadata connection version))
        res (assoc dataset
                   :var-name var-name
                   :version version :metadata mdata
                   :variable-spec options)]
    (assoc res
           :index
           (wrappers-fun (proto/index connection var-name mdata options) res))))

(defn- ^ucar.ma2.Array bytes-to-array
  "Coerces a byte-buffer to a new Array with given data type and shape."
  [^ByteBuffer byte-buffer ^DataType dtype shape]
  ;; todo do we have unnecessary copying here?
  (Array/factory dtype (int-array shape) byte-buffer))

(defn hash->slab [hash chunk-store dtype slice]
  (assert hash)
  (-> (proto/read-chunk chunk-store hash)
      (bytes-to-array dtype (slice/get-shape slice))
      (slab/->Slab slice)))

(defn blank-slab [metadata var-name dtype slice]
  (->> (variable/get-fill metadata var-name)
       (slab/empty dtype slice)))

(defn chunk-at
  "Redefined here so we can instrument it."
  ([index coordinate]
   (proto/chunk-at index coordinate))
  ([index coordinate version-id]
   (proto/chunk-at index coordinate version-id)))

(defn- get-base-chunk [my-current-hash index parent-index store coordinate]
  (let [{:keys [metadata var-name]} (proto/target index)
        dtype (variable/get-dtype metadata var-name)
        chunk-slice (variable/get-chunk-slice metadata var-name coordinate)]
    (if my-current-hash
      (hash->slab my-current-hash store dtype chunk-slice)
      (if-let [parent-hash (and parent-index
                                (chunk-at parent-index coordinate))]
        (let [{parent-metadata :metadata} (proto/target parent-index)]
          (->> (variable/get-chunk-slice parent-metadata var-name coordinate)
               (hash->slab parent-hash store dtype)
               (slab/merge (blank-slab metadata var-name dtype chunk-slice))))
        (blank-slab metadata var-name dtype chunk-slice)))))

(defn- update-chunk!
  [index parent-index version-id store coordinate slab written-already]
  (loop [my-current-hash (chunk-at index coordinate version-id)]
    (let [bc (get-base-chunk my-current-hash index
                             parent-index store coordinate)
          slab (slab/merge bc slab)
          hash (chunk/generate-id slab)
          ;; fixme implement ref-counting
          ref-count -1]
      ;; Write the chunk only if we need to (as far as we know).
      (when-not (or (written-already hash)
                    (= hash my-current-hash))
        (->> slab
             :data
             .getDataAsByteBuffer
             ;; TODO: The following line fixes a bug in ArrayChar, where
             ;; .getDataAsByteBuffer doesn't rewind its position index. We
             ;; should make sure that this doesn't accidentally rewind
             ;; past the start of a chunk. We should also fix this
             ;; upstream.
             .rewind
             ;; write or re-write chunk (generating new chunk ID)
             (proto/write-chunk store hash ref-count)))

      (if (proto/write-index index coordinate my-current-hash hash)
        hash
        ;; If write-index returned nil, the transaction was aborted
        ;; because another writer slipped in ahead of us. Re-merge this chunk.
        (let [sha1 (chunk-at index coordinate version-id)]
          (log/tracef (str "Retrying chunk update transaction at coordinate %s, "
                           "new sha1 %s") (pr-str coordinate) sha1)
          (recur sha1))))))

(defn write-variable
  "Writes the slabs to a specific index.

  We can support multiple machines writing to the same chunk because
  we instantiate the new index from metadata that already contains the
  new index-ids.

  slabs is a (lazy-)seq of Slab"
  [store index parent-index slabs]
  ;; todo use options to create index
  (let [{:keys [metadata var-name]} (proto/target index)
        {parent-metadata :metadata} (when parent-index
                                      (proto/target parent-index))
        written-chunks (atom #{})
        update-fn (fn [slab coordinate]
                    (log/tracef "Updating chunk at %s" (pr-str coordinate))
                    (let [written-chunk
                          (update-chunk!
                            index parent-index (:version-id metadata) store
                            coordinate slab #(contains? @written-chunks %))]
                      (swap! written-chunks conj written-chunk)))]
    (log/debugf "Writing to variable %s. Metadata: %s, parent %s"
                var-name (pr-str metadata) (pr-str parent-metadata))
    (doseq [s slabs]
      (log/debugf "Writing slice %s for variable: %s"
                 (pr-str (:slice s)) var-name)
      (->> (variable/get-chunk-grid-slice metadata var-name)
           (chunk/to-chunk-coordinate (:slice s))
           ;; todo make number of threads configurable
           (utils/npmap (partial update-fn s))
           (dorun)))
    true))
