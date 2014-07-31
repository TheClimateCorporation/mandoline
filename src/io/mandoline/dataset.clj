(ns io.mandoline.dataset
  (:require
    [io.mandoline
     [variable :as variable]
     [utils :as utils]]))

;; Assertions ;;

(defn- valid-type? [type]
  "Checks if the string is in the list of supported types"
  (let [supported #{"byte" "char" "short" "int" "long" "float" "double"}]
    (contains? supported type)))

(defn- valid-fill? [attrs]
  "Checks if the fill value is valid"
  (let [fill (:fill-value attrs)
        allowed #{"double" "float"}]
    (and (number? fill)
         (or (not (Double/isNaN fill))
             (and (Double/isNaN fill)
                  (contains? allowed (:type attrs)))))))

(defn- valid-var? [metadata var-name]
  (every? #(get-in metadata [:variables var-name %]) [:shape :type :fill-value]))

(defn- valid-dimensions? [metadata var-name]
  (every? #(contains? (set (keys (:dimensions metadata))) (keyword %))
          (get-in metadata [:variables var-name :shape])))

;; FIX: Validations should be more specific than "one or more of X
;; failed Y".
(defn validate-dataset-definition
  "The provided dataset map must have the appropriate structure."
  [metadata]
  (utils/attest (every? metadata [:dimensions
                                  :variables
                                  :chunk-dimensions])
                "metadata must contain dimensions, variables, and chunk-dimensions.")
  (utils/attest (->> [:dimensions :chunk-dimensions]
                     (map #(->> %
                                (get metadata)
                                (keys)
                                (sort)))
                     (apply =))
                "there must be a 1:1 mapping from dimensions to chunk-dimensions.")
  (utils/attest (every? #(valid-var? metadata (first %)) (:variables metadata))
                "each variable must contain shape, type and fill-value.")
  (utils/attest (every? #(valid-type? (:type (second %))) (:variables metadata))
                "one or more of the provided variable types are unsupported.")
  (utils/attest (every? #(valid-fill? (second %)) (:variables metadata))
                "one or more of the provided variable fill values are invalid.")
  (utils/attest (every? #(valid-dimensions? metadata (first %)) (:variables metadata))
                "one or more of the provided variable shapes is not a provided dimension."))

(defn- validate-chunk-dims-match
  [child parent]
  (let [[c p] (map :chunk-dimensions [child parent])]
    (utils/attest (->> (keys p)
                       (select-keys c)
                       (= p))
                  "chunk-dimensions must match between versions.")))

(defn- validate-var-type
  [child parent var]
  (utils/attest (->> (map #(get-in % [:variables var :type]) [child parent])
                     (apply =))
                "type for var cannot change between child and parent versions."))

(defn- validate-var-shape
  [child parent var]
  (utils/attest (->> (map #(get-in % [:variables var :shape]) [child parent])
                     (apply =))
                "shape for var cannot change between child and parent versions."))

(defn- validate-var-fill
  [child parent var]
  (utils/attest (->> (map #(get-in % [:variables var :fill-value]) [child parent])
                     (apply utils/nan=))
                "fill for var cannot change between child and parent versions."))

;; Token construction/modification ;;

(defn- inherit-chunk-dims
  [child parent]
  (->> (:chunk-dimensions parent)
       (assoc child :chunk-dimensions)))

(defn- validate-child-parent [child parent var]
  ;; Allow the introduction of new variables (i.e. don't attempt to
  ;; check variables in 'child' that didn't exist in 'parent').
  (when-not (nil? (get-in parent [:variables var]))
    (validate-var-type child parent var)
    (validate-var-shape child parent var)
    (validate-var-fill child parent var)))

(defn inherit
  "Validate a child version's metadata and set up its relationship to
  its parent. Returns the child dataset's modified token."
  [child parent]
  (let [parent-version (:version-id parent)
        ;; copy chunk dimensions from the parent if not defined in the child
        child (-> (if (:chunk-dimensions child)
                    child
                    (inherit-chunk-dims child parent))
                  ;; set up the child's parent pointer
                  (assoc :parent parent-version))]
    (validate-chunk-dims-match child parent)
    (doseq [var (keys (:variables child))]
      (validate-child-parent child parent var))
    child))

(defn create
  "Populate a new dataset's metadata and generate a token to be used
  by subsequent operations. If the metadata does not define e.g. chunk
  dimensions, choose some sane defaults. Returns the new token."
  [metadata]
  ;; TODO: define equilateral chunks with a roughly fixed size
  (assoc metadata :parent nil))

(defn new-version
  [metadata]
  (assoc metadata :version-id (.getTime (java.util.Date.))))
