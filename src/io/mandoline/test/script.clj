(ns io.mandoline.test.script
  "Here, we assume that the dataset that the store-spec points to already
  exists, and we are just instantiating a variable writer, to write arbitrary
  data to arbritrary coordinates.

  The script is called in
  io.mandoline.test.concurrency/lots-of-processes."
  (:use
   [clojure test]
   [clojure.tools.logging :as log])
  (:require
   [clojure.string :as str]
   [clojure.tools.cli :as cli]
   [io.mandoline :as db]
   [io.mandoline.slab :as slab]
   [io.mandoline.slice :as slice]
   [io.mandoline.variable :as var]
   [io.mandoline.utils :as utils]
   [io.mandoline.test.utils :as test-utils]))

(defn- cs->seq [string]
  (->> (str/split string #",") (map #(Integer/parseInt %))))

(def cmd-spec
  [["-h" "--help" "print help and exit."]
   ["-f" "--fill" "the fill value of the data to be written"
    :parse-fn #(read-string %)
    :default 1]
   ["-c" "--coordinates" "Coordinates of the last dimension to write"
    :parse-fn #(cs->seq %)]
   ["-v" "--variable-name" "Variable name of the variable to write"
    :parse-fn #(keyword %)]
   ["-s" "--store-spec" "Store specification of the dataset"
    :parse-fn #(utils/parse-metadata % true)]
   ["-t" "--token" "Token for the dataset to be written to"
    :parse-fn #(utils/parse-metadata % true)]])

(defn- generate-data
  "Generates data based on the variable shape, with the coordinates
  specifying only on the last dimensions."
  [metadata var-name coordinates fill]
  (let [var-slice (var/get-var-slice metadata var-name)
        type (var/get-type metadata var-name)
        start- (vec (butlast (:start var-slice)))
        stop- (vec (butlast (:stop var-slice)))]
    (for [n coordinates
          :let [s (slice/mk-slice (conj start- n) (conj stop- (inc n)))]]
      (test-utils/same-slab type s fill))))

(defn- write [writer var-name data]
  (with-open [var-writer (db/variable-writer writer var-name {:wrappers []})]
    (db/write var-writer data)))

(defn -main
  [& argv]
  (let [[{:keys [help fill coordinates variable-name
                 store-spec token] :as options} _ banner]
    (apply cli/cli argv cmd-spec)]
    (log/debugf "slave got options: %s " (pr-str options))
    (when (or help
              (some not [fill coordinates variable-name
                         store-spec token]))
      (println banner)
      (System/exit -1))
    (let [data (generate-data token variable-name coordinates fill)
          writer (db/token->dataset-writer store-spec token)]
      (log/debugf "slave writer %s" (pr-str writer))
      (println "r")
      (flush)
      (let [input (read-line)]
        (when (= input "go")
          (log/debugf "Slave got 'go' instruction")
          (println "writing")
          (flush)
          (write writer variable-name data)
          (log/debugf "Slave wrote data")
          (println "done")
          (flush)
          (log/debugf "Slave wrote done")
          (Thread/sleep 1000)
          (log/debugf "Slave exiting")
          (System/exit 0)))
      (System/exit -1))))
