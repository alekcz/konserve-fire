(ns konserve-fire.core
  "Address globally aggregated immutable key-value store(s)."
  (:require [clojure.core.async :as async]
            [konserve.serializers :as ser]
            [konserve.compressor :as comp]
            [konserve.encryptor :as encr]
            [hasch.core :as hasch]
            [konserve-fire.io :as io]
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
                                        -keys]]
            [konserve.storage-layout :refer [Layout2]])
  (:import  [java.io ByteArrayOutputStream]))

(set! *warn-on-reflection* 1)

(def store-layout 1)

(defn str-uuid 
  [key] 
  (str (hasch/uuid key))) 

(defn prep-ex 
  [^String message ^Exception e]
  (.printStackTrace e)
  (ex-info message {:error (.getMessage e) :cause (.getCause e) :trace (.getStackTrace e)}))

(defn prep-stream 
  [stream]
  { :input-stream stream
    :size nil})

(defrecord FireStore [store default-serializer serializers compressor encryptor read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? 
    [this key] 
      (let [res-ch (async/chan 1)]
        (async/thread
          (try
            (async/put! res-ch (io/it-exists? store (str-uuid key)))
            (catch Exception e (async/put! res-ch (prep-ex "Failed to determine if item exists" e)))))
        res-ch))

  (-get 
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[header res] (io/get-it-only store (str-uuid key))]
            (if (some? res) 
              (let [rserializer (ser/byte->serializer (get header 1))
                    rcompressor (comp/byte->compressor (get header 2))
                    rencryptor  (encr/byte->encryptor  (get header 3))
                    reader (-> rserializer rencryptor rcompressor)
                    data (-deserialize reader read-handlers res)]
                (async/put! res-ch data))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value from store" e)))))
      res-ch))

  (-get-meta 
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[header res] (io/get-meta store (str-uuid key))]
            (if (some? res) 
              (let [rserializer (ser/byte->serializer (get header 1))
                    rcompressor (comp/byte->compressor (get header 2))
                    rencryptor  (encr/byte->encryptor  (get header 3))
                    reader (-> rserializer rencryptor rcompressor)
                    data (-deserialize reader read-handlers res)] 
                (async/put! res-ch data))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve metadata from store" e)))))
      res-ch))

  (-update-in 
    [this key-vec meta-up-fn up-fn args]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[fkey & rkey] key-vec
                [[mheader ometa'] [vheader oval']] (io/get-it store (str-uuid fkey))
                old-val [(when ometa'
                            (let [mserializer (ser/byte->serializer  (get mheader 1))
                                  mcompressor (comp/byte->compressor (get mheader 2))
                                  mencryptor  (encr/byte->encryptor  (get mheader 3))
                                  reader (-> mserializer mencryptor mcompressor)]
                              (-deserialize reader read-handlers ometa')))
                         (when oval'
                            (let [vserializer (ser/byte->serializer  (get vheader 1))
                                  vcompressor (comp/byte->compressor (get mheader 2))
                                  vencryptor  (encr/byte->encryptor  (get mheader 3))
                                  reader (-> vserializer vencryptor vcompressor)]
                              (-deserialize reader read-handlers oval')))]            
                [nmeta nval] [(meta-up-fn (first old-val)) 
                              (if rkey (apply update-in (second old-val) rkey up-fn args) (apply up-fn (second old-val) args))]
                serializer (get serializers default-serializer)
                writer (-> serializer compressor encryptor)
                ^ByteArrayOutputStream mbaos (ByteArrayOutputStream.)
                ^ByteArrayOutputStream vbaos (ByteArrayOutputStream.)]
            (when nmeta 
              (.write mbaos ^byte store-layout)
              (.write mbaos ^byte (ser/serializer-class->byte (type serializer)))
              (.write mbaos ^byte (comp/compressor->byte compressor))
              (.write mbaos ^byte (encr/encryptor->byte encryptor))
              (-serialize writer mbaos write-handlers nmeta))
            (when nval 
              (.write vbaos ^byte store-layout)
              (.write vbaos ^byte (ser/serializer-class->byte (type serializer)))
              (.write vbaos ^byte (comp/compressor->byte compressor))
              (.write vbaos ^byte (encr/encryptor->byte encryptor))
              (-serialize writer vbaos write-handlers nval))    
            (io/update-it store (str-uuid fkey) [(.toByteArray mbaos) (.toByteArray vbaos)])
            (async/put! res-ch [(second old-val) nval]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to update/write value in store" e)))))
        res-ch))

  (-assoc-in [this key-vec meta val] (-update-in this key-vec meta (fn [_] val) []))

  (-dissoc 
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (io/delete-it store (str-uuid key))
          (async/close! res-ch)
          (catch Exception e (async/put! res-ch (prep-ex "Failed to delete key-value pair from store" e)))))
        res-ch))

  PBinaryAsyncKeyValueStore
  (-bget 
    [this key locked-cb]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[header res] (io/get-it-only store (str-uuid key))]
            (if (some? res) 
              (let [rserializer (ser/byte->serializer (get header 1))
                    rcompressor (comp/byte->compressor (get header 2))
                    rencryptor  (encr/byte->encryptor  (get header 3))
                    reader (-> rserializer rencryptor rcompressor)
                    data (-deserialize reader read-handlers res)]
                (async/put! res-ch (locked-cb (prep-stream data))))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve binary value from store" e)))))
      res-ch))

  (-bassoc 
    [this key meta-up-fn input]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[[mheader old-meta'] [_ old-val]] (io/get-it store (str-uuid key))
                old-meta (when old-meta' 
                            (let [mserializer (ser/byte->serializer  (get mheader 1))
                                  mcompressor (comp/byte->compressor (get mheader 2))
                                  mencryptor  (encr/byte->encryptor  (get mheader 3))
                                  reader (-> mserializer mencryptor mcompressor)]
                              (-deserialize reader read-handlers old-meta')))           
                new-meta (meta-up-fn old-meta) 
                serializer (get serializers default-serializer)
                writer (-> serializer compressor encryptor)
                ^ByteArrayOutputStream mbaos (ByteArrayOutputStream.)
                ^ByteArrayOutputStream vbaos (ByteArrayOutputStream.)]
            (when new-meta 
              (.write mbaos ^byte store-layout)
              (.write mbaos ^byte (ser/serializer-class->byte (type serializer)))
              (.write mbaos ^byte (comp/compressor->byte compressor))
              (.write mbaos ^byte (encr/encryptor->byte encryptor))
              (-serialize writer mbaos write-handlers new-meta))
            (when input
              (.write vbaos ^byte store-layout)
              (.write vbaos ^byte (ser/serializer-class->byte (type serializer)))
              (.write vbaos ^byte (comp/compressor->byte compressor))
              (.write vbaos ^byte (encr/encryptor->byte encryptor))
              (-serialize writer vbaos write-handlers input))  
            (io/update-it store (str-uuid key) [(.toByteArray mbaos) (.toByteArray vbaos)])
            (async/put! res-ch [old-val input]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to write binary value in store" e)))))
        res-ch))

  PKeyIterable
  (-keys 
    [_]
    (let [res-ch (async/chan)]
      (async/thread
        (try
          (let [key-stream (io/get-keys store)
                keys' (when key-stream
                        (for [[header k] key-stream]
                          (let [rserializer (ser/byte->serializer (get header 1))
                                rcompressor (comp/byte->compressor (get header 2))
                                rencryptor  (encr/byte->encryptor  (get header 3))
                                reader (-> rserializer rencryptor rcompressor)]
                            (-deserialize reader read-handlers k))))
                keys (doall (map :key keys'))]
            (doall
              (map #(async/put! res-ch %) keys))
            (async/close! res-ch)) 
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve keys from store" e)))))
        res-ch))
        
  Layout2      
  (-get-raw-meta [this key]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (io/raw-get-meta store (str-uuid key))]
            (if res
              (async/put! res-ch res)
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve raw metadata from store" e)))))
      res-ch))
  (-put-raw-meta [store key binary]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (io/raw-update-meta store (str-uuid key) binary)]
            (if res
              (async/put! res-ch res)
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to write raw metadata to store" e)))))
      res-ch))
  (-get-raw-value [store key]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (io/raw-get-it-only store (str-uuid key))]
            (if res
              (async/put! res-ch res)
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve raw value from store" e)))))
      res-ch))
  (-put-raw-value [store key binary]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (io/raw-update-it-only store (str-uuid key) binary)]
            (if res
              (async/put! res-ch res)
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to write raw value to store" e)))))
      res-ch)))

(defn new-fire-store
  "Creates an new store based on Firebase's realtime database."
  [env & {:keys [root db default-serializer serializers compressor encryptor read-handlers write-handlers]
          :or  {root "/konserve-fire"
                db nil
                default-serializer :FressianSerializer
                compressor comp/lz4-compressor
                encryptor encr/null-encryptor
                read-handlers (atom {})
                write-handlers (atom {})}}]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [auth (when env (fire-auth/create-token env))
                final-db (if (nil? db) (:project-id auth) db)
                final-root (if (str/starts-with? root "/") root (str "/" root))]
            (when-not final-db 
              (throw (prep-ex "Invalid database" (Exception. "No database specified and one could not be automatically determined." ))))
            (when-not auth
              (throw (prep-ex "Authentication failed" (Exception. (str "Authentication failed using environment variablbe: " env)))))              
            (async/put! res-ch
              (map->FireStore { :store {:db final-db :auth auth :root final-root}
                                :default-serializer default-serializer
                                :serializers (merge ser/key->serializer serializers)
                                :compressor compressor
                                :encryptor encryptor
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
