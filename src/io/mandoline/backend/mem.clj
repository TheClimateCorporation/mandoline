(ns io.mandoline.backend.mem
  "Mandoline backend that uses Clojure atoms for single-process,
  in-memory storage. This backend is provided as a reference
  implementation for small-scale development and testing."
  (:require
    [clojure.string :as string]
    [clojure.tools.logging :as log]
    [io.mandoline.utils :as utils]
    [io.mandoline.impl.protocol :as proto])
  (:import
    [java.nio ByteBuffer]
    [org.joda.time DateTime]))

;; ---------------------
;; Notably, this store shares chunks across all datasets
;;
;; {:chunks {"sha1" bytes "sha2" bytes}
;;
;;  :datasets
;;   {"dataset1"
;;    {:versions {"version1" {:metadata "foooo" :timestamp MyDateTime}
;;                "version2" {:metadata "baaar" :timestamp MyDateTime}}
;;
;;     :indices
;;     {"version1" {:var-name1 {[0 0 1] "mysha1"
;;                              [0 0 2] "mysha2"}
;;                  :var-name2 {[0 0 1] "mysha1"
;;                              [0 0 2] "mysha2"}}}}}}

(defonce ^:dynamic ^:private available-stores (atom {}))

(defn- find-index-lower-than [version-cache state version-id]
  (when-let [f (->> state (filter #(< (first %) version-id)) first)]
    (let [version-id (first f)]
      (if (utils/committed? version-cache version-id)
        (peek f)
        (recur version-cache state version-id)))))

(deftype MemIndex [store dataset metadata var-name version-cache]
  proto/Index

  (target [_]
    {:metadata metadata :var-name var-name})

  (chunk-at [_ coordinate]
     (log/tracef "Reading index %s" (pr-str coordinate))
     (when-let [m (get-in @store [:datasets dataset :indices var-name coordinate])]
       (or (->> metadata :version-id (get m))
           (find-index-lower-than version-cache m (:version-id metadata)))))

  (chunk-at [_ coordinate version-id]
     (log/tracef "Reading index %s for version %s" (pr-str coordinate) version-id)
     (get-in @store [:datasets dataset :indices var-name coordinate (:version-id metadata)]))

  (write-index [this coordinates old-hash new-hash]
    (let [v (:version-id metadata)]
      (log/tracef "Writing index %s, assuming old value = %s, version = %s"
                  new-hash old-hash v)
      (->
        (swap! store update-in
               [:datasets dataset :indices var-name coordinates]
               #(cond
                  (nil? %) (sorted-map-by > v new-hash)
                  (= (get % v) old-hash) (assoc % v new-hash)
                  :else %))
        (get-in [:datasets dataset :indices var-name coordinates v])
        (= new-hash))))

  (flush-index [_]
    (log/debug "MemIndex flushing resources")))

(deftype MemChunkStore [store]
  proto/ChunkStore

  (read-chunk [_ hash]
    (log/tracef "Reading chunk %s" hash)
    ;; We need to return a new view of the stored buffer here.
    ;; It shares the same backing array but has it's own position markers.
    (if-let [data (get-in @store [:chunks hash :blob])]
      (.slice data)
      (throw
        (IllegalArgumentException.
          (format "No chunk was found for hash %s" hash)))))

  (chunk-refs [_ hash]
    (log/tracef "Getting ref count for chunk %s" hash)
    (if-let [ref-count (get-in @store [:chunks hash :ref])]
      ref-count
      (throw
        (IllegalArgumentException.
          (format "No chunk was found for hash %s" hash)))))

  (write-chunk [_ hash ref-count bytes]
    (when (or (empty? hash) (not (string? hash)))
      (throw
        (IllegalArgumentException. "hash must be a non-empty string")))
    (when-not (integer? ref-count)
      (throw
        (IllegalArgumentException. "ref-count must be an integer")))
    (when-not (instance? ByteBuffer bytes)
      (throw
        (IllegalArgumentException. "bytes must be a ByteBuffer instance")))
    (when-not (pos? (.remaining bytes))
      (throw
        (IllegalArgumentException. "Chunk has no remaining bytes")))
    (log/tracef "Writing chunk %s" hash)
    (swap! store assoc-in [:chunks hash] {:ref ref-count :blob bytes})
    nil) ; return nil

  (update-chunk-refs [_ hash delta]
    (when (or (empty? hash) (not (string? hash)))
      (throw
        (IllegalArgumentException. "hash must be a non-empty string")))
    (when-not (integer? delta)
      (throw
        (IllegalArgumentException. "delta must be an integer")))
    (swap! store update-in [:chunks hash :ref] + delta)
    nil)) ; return nil

(deftype MemConnection [store dataset]
  proto/Connection

  (index [this var-name metadata options]
    (->MemIndex store dataset metadata var-name
                (utils/mk-version-cache
                  (map #(-> % :version Long/parseLong)
                       (proto/versions this {:metadata? false})))))

  (write-version [_ metadata]
    (swap! store assoc-in
           [:datasets dataset :versions (:version-id metadata)]
           {:metadata metadata :timestamp (DateTime.)}))

  (chunk-store [_ options] (->MemChunkStore store))

  (get-stats [_]
    (assert false "get-stats is not implemented yet"))

  (metadata [_ version]
    (get-in @store
            [:datasets dataset :versions (Long/parseLong version) :metadata]))

  (versions [_ {:keys [limit metadata?]}]
    (let [result (->> (get-in @store [:datasets dataset :versions])
                      (reduce-kv
                       (fn [res version-id data]
                         (conj res (merge
                                    {:timestamp (:timestamp data)
                                     :version (str version-id)}
                                    (when metadata?
                                      {:metadata (:metadata data)}))))
                       ())
                      (sort-by :timestamp)
                      reverse)]
      (if limit (take limit result) result))))

(deftype MemSchema [store]
  proto/Schema

  (create-dataset [_ name]
    (when-not (and (string? name) (not (string/blank? name)))
      (throw
        (IllegalArgumentException. "dataset name must be a non-empty string")))
    (swap!
      store
      update-in
      [:datasets name]
      #(if (nil? %)
         {:indices {} :versions {}}
         (throw
           (IllegalStateException.
             (format "dataset with name \"%s\" already exists." name)))))
    nil)

  (destroy-dataset [_ name]
    (swap! store update-in [:datasets] dissoc name)
    nil)

  (list-datasets [_]
    (or (-> store deref :datasets keys) (list)))

  (connect [_ dataset-name]
    (when-not (get-in (deref store) [:datasets dataset-name])
      (throw
        (IllegalArgumentException.
          (format "dataset with name \"%s\" does not exist." dataset-name))))
    (->MemConnection store dataset-name)))

(defn create-schema [root]
  (swap! available-stores assoc root (atom {:chunks {} :datasets {}})))

(defn destroy-schema [root]
  (swap! available-stores dissoc root))

(defn destroy-all-schemas []
  (reset! available-stores {}))

(defn mk-schema [store-spec]
  (let [root (if (:db-version store-spec)
               (string/join "." (:db-version store-spec) (:root store-spec))
               (:root store-spec))]
    (->MemSchema
      (or (@available-stores root)
          ((create-schema root) root)))))
