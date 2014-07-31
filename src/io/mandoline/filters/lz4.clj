(ns io.mandoline.filters.lz4
  "Basic LZ4/LZ4HC filter format
  CompressedMarker,[UncompressedLength,CompressedBytes..|RawBytes..]

  CompressedMarker (Byte)
  0 -> data was not compressed!

       RawBytes [Bytes]
       the remaining of the buffer is the raw uncompressed data

  1 -> data was compressed!
       the maining of the buffer is UncompressedLength,CompressedBytes

       UncompressedLength (Int32 LittleEndian)
       Size of the data after decompression
       the remaining of the buffer is the raw uncompressed data

       CompressedBytes [Byte]
       the remaining of the buffer is LZ4/LZ4HC compressed data"
  (:import
    [java.nio ByteBuffer ByteOrder]
    [net.jpountz.lz4 LZ4Factory LZ4Compressor LZ4Decompressor]))

(def ^:private ^LZ4Factory lz4factory (LZ4Factory/fastestInstance))

(def ^:private ^:const min-compression-ratio 1.2)

(defn- mk-compress
  [^LZ4Compressor compressor]
  (fn [^ByteBuffer decompressed-buffer]
    (let [decompressed-length (.remaining decompressed-buffer)
          max-compressed-length (.maxCompressedLength compressor decompressed-length)
          compressed-buffer (-> (ByteBuffer/allocate (+ 5 max-compressed-length))
                                (.order ByteOrder/LITTLE_ENDIAN))
          compressed-length (.compress compressor
                                       (.array decompressed-buffer)
                                       (+ (.arrayOffset decompressed-buffer)
                                          (.position decompressed-buffer))
                                       decompressed-length
                                       (.array compressed-buffer)
                                       5
                                       max-compressed-length)
          compression-ratio (/ decompressed-length compressed-length)]
      (if (< compression-ratio min-compression-ratio)
        (-> compressed-buffer     ;; return marked as uncompressed
            (.put (byte 0))
            (.put (.array decompressed-buffer)
                  (+ (.arrayOffset decompressed-buffer)
                     (.position decompressed-buffer))
                  decompressed-length)
            (.flip))
        (-> compressed-buffer             ;; return marked as compressed
            (.put (byte 1))
            (.putInt decompressed-length)
            (.position (+ (.position compressed-buffer) compressed-length))
            (.flip))))))

(def ^:private compress (mk-compress (.fastCompressor lz4factory)))
(def ^:private compress-hc (mk-compress (.highCompressor lz4factory)))

(defn- decompress
  [^ByteBuffer compressed-buffer]
  (if (== 0 (.get compressed-buffer))
    (.slice compressed-buffer)                   ;; not compressed
    (let [decompressed-length (-> compressed-buffer
                                  (.order ByteOrder/LITTLE_ENDIAN)
                                  (.getInt))
          decompressed-buffer (ByteBuffer/allocate decompressed-length)]
      (.decompress (.decompressor lz4factory)
                   (.array compressed-buffer)
                   (+ (.arrayOffset compressed-buffer) (.position compressed-buffer))
                   (.array decompressed-buffer)
                   0
                   decompressed-length)
      (-> decompressed-buffer
          (.position decompressed-length)
          (.flip)))))

(defn filter-apply
  [metadata chunk]
  (compress chunk))

(defn filter-reverse
  [chunk]
  (decompress chunk))

(defn filter-apply-hc
  [metadata chunk]
  (compress-hc chunk))
