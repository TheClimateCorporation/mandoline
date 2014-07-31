(ns io.mandoline.filters.deflate
  "Basic DEFLATE filter format
  CompressedMarker,[UncompressedLength,CompressedBytes..|RawBytes..]

  CompressedMarker (Byte)
  0 -> data was not compressed!

       RawBytes [Bytes]
       the remaining of the buffer is the raw uncompressed data

  1 -> data was compressed!
       the maining of the buffer is CompressedBytes

       UncompressedLength (Int32 LittleEndian)
       Size of the data after decompression
       the remaining of the buffer is the raw uncompressed data

       CompressedBytes [Byte]
       the remaining of the buffer is DEFLATE compressed data"
  (:import
    [java.io ByteArrayInputStream]
    [java.nio ByteBuffer ByteOrder]
    [java.util.zip Deflater Inflater DeflaterInputStream InflaterInputStream]))

(def ^:private ^:const min-compression-ratio 1.2)

(def ^:private ^:const default-compression-level 6)

(defn- deflate
  [compression-level ^ByteBuffer decompressed-buffer]
  (let [decompressed-length (.remaining decompressed-buffer)
        decompressed-stream (ByteArrayInputStream. (.array decompressed-buffer)
                                                   (+ (.arrayOffset decompressed-buffer)
                                                      (.position decompressed-buffer))
                                                   decompressed-length)
        deflater (DeflaterInputStream. decompressed-stream (Deflater. compression-level))
        compressed-buffer (-> (ByteBuffer/allocate 65536)
                              (.order ByteOrder/LITTLE_ENDIAN))
        compressed-length (.read deflater (.array compressed-buffer) 1 65535)
        compression-ratio (/ decompressed-length compressed-length)]
    (if (< compression-ratio min-compression-ratio)
      (-> compressed-buffer               ;; return marked as uncompressed
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
            (.flip)))))

(defn- inflate
  [^ByteBuffer compressed-buffer]
  (if (== 0 (.get compressed-buffer))
    (.slice compressed-buffer)                   ;; not compressed
    (let [decompressed-length (-> compressed-buffer
                                  (.order ByteOrder/LITTLE_ENDIAN)
                                  (.getInt))
          decompressed-buffer (ByteBuffer/allocate decompressed-length)
          compressed-stream (ByteArrayInputStream. (.array compressed-buffer)
                                                   (+ (.arrayOffset compressed-buffer)
                                                      (.position compressed-buffer))
                                                   (.remaining compressed-buffer))
          inflater (InflaterInputStream. compressed-stream (Inflater.))
          compressed-length (.read inflater (.array decompressed-buffer) 0 decompressed-length)]
      (-> decompressed-buffer
          (.position compressed-length)
          (.flip)))))

(defn filter-apply
  [metadata chunk]
  (deflate (:deflate-level metadata default-compression-level) chunk))

(defn filter-reverse
  [chunk]
  (inflate chunk))
