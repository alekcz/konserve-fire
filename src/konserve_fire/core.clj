(ns konserve-fire.core
  "Address globally aggregated immutable key-value store(s)."
  (:require [clojure.core.async :as async]
            [konserve.serializers :as ser]
            [hasch.core :as hasch]
            [fire.auth :as fire-auth]
            [fire.core :as fire]
            [clojure.string :as str]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get -get-meta
                                        -update-in -assoc-in -dissoc
                                        PBinaryAsyncKeyValueStore
                                        -bassoc -bget
                                        -serialize -deserialize
                                        PKeyIterable
                                        -keys]])
  (:import  [java.io ByteArrayInputStream StringWriter]
            [java.util Base64 Base64$Decoder Base64$Encoder]))

(set! *warn-on-reflection* 1)

(def ^Base64$Encoder b64encoder (. Base64 getEncoder))
(def ^Base64$Decoder b64decoder (. Base64 getDecoder))
(def store-version 1)
(def serializer 1)
(def compressor 0)
(def encryptor 0)

(defn headers []
  (.encodeToString b64encoder ^"[B" 
    (byte-array [(byte store-version) (byte serializer) (byte compressor) (byte encryptor)])))

(defn chunk-str [string]
  (let [len (count string)
        chunk-map  (if (> len 5000000) 
                      (let [chunks (str/split string #"(?<=\G.{5000000})")]
                        (for [n (range (count chunks))] 
                          {(str "p" n) (nth chunks n)}))
                      {:headers (headers)
                       :p0 string})]
    (apply merge {} chunk-map)))

(defn combine-str [data-str]
  (->> (dissoc data-str :headers) (into (sorted-map)) vals vec str/join))

(defn prep-write 
  [data]
  (let [[meta val] data]
    {:data (chunk-str val)
     :meta meta
     :type "string"}))

(defn prep-bwrite 
  [data]
  (let [[meta val] data]
    {:data (chunk-str (.encodeToString b64encoder ^"[B" val))
     :meta meta
     :type "binary"}))

(defn prep-read 
  [data']
  (let [data (combine-str (:data data'))
        meta (:meta data')
        type (:type data')]
    (case type
      "string" [meta data]
      "binary" [meta (.decode b64decoder ^String data)]
      nil)))

(defn it-exists? 
  [store id]
    (let [resp (fire/read (:db store) (str (:root store) "/" id "/data") (:auth store) {:query {:shallow true}})]
    (some? resp)))
  
(defn get-it 
  [store id]
  (let [resp (fire/read (:db store) (str (:root store) "/" id) (:auth store))]
    (prep-read resp)))

(defn get-meta
  [store id]
  (let [resp (fire/read (:db store) (str (:root store) "/" id "/meta") (:auth store))]
    resp))

(defn update-it 
  [store id data]
  (fire/update! (:db store) (str (:root store) "/" id) (prep-write data) (:auth store) {:print "silent"}))

(defn bupdate-it 
  [store id data]
  (fire/update! (:db store) (str (:root store) "/" id) (prep-bwrite data) (:auth store) {:print "silent"}))

(defn delete-it 
  [store id]
  (fire/delete! (:db store) (str (:root store) "/" id) (:auth store)))

(defn get-keys 
  [store]
  (let [resp (fire/read (:db store) (str (:root store)) (:auth store) {:query {:shallow true}})
        key-stream (seq (keys resp))
        getmeta (fn [id] (get-meta store (name id)))]
    (map getmeta key-stream)))

(defn str-uuid 
  [key] 
  (str (hasch/uuid key))) 

(defn prep-ex 
  [^String message ^Exception e]
  ;(.printStackTrace e)
  (ex-info message {:error (.getMessage e) :cause (.getCause e) :trace (.getStackTrace e)}))

(defn prep-stream 
  [bytes]
  { :input-stream  (ByteArrayInputStream. bytes) 
    :size (count bytes)})

(defrecord FireStore [store serializer read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? 
    [this key] 
      (let [res-ch (async/chan 1)]
        (async/thread
          (try
            (async/put! res-ch (it-exists? store (str-uuid key)))
            (catch Exception e (async/put! res-ch (prep-ex "Failed to determine if item exists" e)))))
        res-ch))

  (-get 
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-it store (str-uuid key))]
            (if (some? res) 
              (async/put! res-ch (-deserialize serializer read-handlers (second res)))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value from store" e)))))
      res-ch))

  (-get-meta 
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-meta store (str-uuid key))]
            (if (some? res) 
              (async/put! res-ch (-deserialize serializer read-handlers res))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value metadata from store" e)))))
      res-ch))

  (-update-in 
    [this key-vec meta-up-fn up-fn args]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[fkey & rkey] key-vec
                [ometa' oval'] (get-it store (str-uuid fkey))
                old-val [(when ometa'
                          (-deserialize serializer read-handlers ometa'))
                         (when oval'
                          (-deserialize serializer read-handlers oval'))] 
                [nmeta nval] [(meta-up-fn (first old-val)) 
                         (if rkey (apply update-in (second old-val) rkey up-fn args) (apply up-fn (second old-val) args))]
                ^StringWriter mbaos (StringWriter.)
                ^StringWriter vbaos (StringWriter.)]
            (when nmeta (-serialize serializer mbaos write-handlers nmeta))
            (when nval (-serialize serializer vbaos write-handlers nval))
            (update-it store (str-uuid fkey) [(.toString mbaos) (.toString vbaos)])
            (async/put! res-ch [(second old-val) nval]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to update/write value in store" e)))))
        res-ch))

  (-assoc-in 
    [this key-vec meta nval] 
    (-update-in this key-vec meta (fn [_] nval) []))

  (-dissoc 
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (delete-it store (str-uuid key))
          (async/close! res-ch)
          (catch Exception e (async/put! res-ch (prep-ex "Failed to delete key-value pair from store" e)))))
        res-ch))

  PBinaryAsyncKeyValueStore
  (-bget 
    [this key locked-cb]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-it store (str-uuid key))]
            (if (some? res) 
              (async/put! res-ch (locked-cb (prep-stream (second res))))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve binary value from store" e)))))
      res-ch))

  (-bassoc 
    [this key meta-up-fn input]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [old-val (get-it store (str-uuid key))
                old-meta (-deserialize serializer read-handlers (first old-val))
                new-meta (meta-up-fn old-meta)
                ^StringWriter mbaos (StringWriter.)]
            (-serialize serializer mbaos write-handlers new-meta)
            (bupdate-it store (str-uuid key) [(.toString mbaos) input])
            (async/put! res-ch [(second old-val) input]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to update/write binary value in store" e)))))
        res-ch))

  PKeyIterable
  (-keys 
    [_]
    (let [res-ch (async/chan)]
      (async/thread
        (try
          (let [key-stream (get-keys store)
                keys (when key-stream
                        (for [k key-stream]
                          (:key (-deserialize serializer read-handlers k))))]
            (doall
              (map #(async/put! res-ch %) keys)))
          (async/close! res-ch) 
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve keys from store" e)))))
        res-ch)))

(defn new-fire-store
  "Creates an new store based on Firebase's realtime database."
  [env & {:keys [root db read-handlers write-handlers]
          :or  {root "/konserve-fire"
                db nil
                read-handlers (atom {})
                write-handlers (atom {})}}]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [auth (fire-auth/create-token env)
                final-db (if (nil? db) (:project-id auth) db)
                final-root (if (str/starts-with? root "/") root (str "/" root))]
            (async/put! res-ch
              (map->FireStore { :store {:db final-db :auth auth :root final-root}
                                :serializer (ser/string-serializer)
                                :read-handlers read-handlers
                                :write-handlers write-handlers
                                :locks (atom {})})))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to connect to store" e)))))          
        res-ch))

(defn delete-store [fire-store]
  (let [res-ch (async/chan 1)]
    (async/thread
      (try
        (let [store (:store fire-store)]
          (fire/delete! (:db store) (str (:root store)) (:auth store))
          (async/close! res-ch))
        (catch Exception e (async/put! res-ch (prep-ex "Failed to delete store" e)))))          
        res-ch))
