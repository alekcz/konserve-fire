(ns konserve-fire.core
  "Address globally aggregated immutable key-value store(s)."
  (:require [clojure.core.async :as async]
            [konserve.serializers :as ser]
            [hasch.core :as hasch]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get -get-meta
                                        -update-in -assoc-in -dissoc
                                        PBinaryAsyncKeyValueStore
                                        -bassoc -bget
                                        PKeyIterable
                                        -keys]]
            [incognito.edn :refer [read-string-safe]]
            [fire.auth :as fire-auth]
            [fire.core :as fire]
            [clojure.string :as str])
  (:import  [java.util Base64 Base64$Decoder Base64$Encoder]
            [java.io ByteArrayInputStream]))

(set! *warn-on-reflection* 1)

(def ^Base64$Encoder b64encoder (. Base64 getEncoder))
(def ^Base64$Decoder b64decoder (. Base64 getDecoder))

(defn chunk-str [string]
  (let [len (count string)
        chunk-map  (if (> len 5000000) 
                      (let [chunks (str/split string #"(?<=\G.{5000000})")]
                        (for [n (range (count chunks))] 
                          {(str "p" n) (nth chunks n)}))
                      {:p0 string})]
    (apply merge {} chunk-map)))

(defn serialize [id data]
  (let [k {:d (-> data first pr-str)}
        v {:meta {:d (-> data first pr-str)
                  :type "meta"}
           :data {:d (-> data second pr-str chunk-str)
                  :type  "data"}}]
    {(str "/keys/" id) k (str "/data/" id) v}))

(defn bserialize [id data]
  (let [finaldata (.encodeToString b64encoder ^"[B" (second data))
        k {:d (-> data first pr-str)}
        v {:meta {:d (-> data first pr-str)
                  :type "meta"}
           :data {:d (-> finaldata pr-str chunk-str)
                  :type "binary"}}]
    {(str "/keys/" id) k (str "/data/" id) v}))

(defn deserialize [data' read-handlers]
  (case (:type data')
    "data" 
      (let [joined (-> data' :d vals vec str/join)]
        (read-string-safe @read-handlers joined))
    "binary" 
      (let [joined (-> data' :d vals vec str/join)
            string (read-string-safe @read-handlers joined)]
        (.decode b64decoder ^String string))
    (read-string-safe @read-handlers (:d data'))))
    
(defn item-exists? [db id]
  (let [resp (fire/read (:db db) (str (:root db) "/data/" id) (:auth db) {:query {:shallow true} :pool (:pool db)})]
    (some? resp)))

(defn get-item [db id read-handlers]
  (let [resp (fire/read (:db db) (str (:root db) "/data/" id) (:auth db) {:pool (:pool db)})]
    [(deserialize (:meta resp) read-handlers) (deserialize (:data resp) read-handlers)]))

(defn get-item-only [db id read-handlers]
  (let [resp (fire/read (:db db) (str (:root db) "/data/" id "/data") (:auth db) {:pool (:pool db)})]
    (deserialize resp read-handlers)))

(defn get-meta-only [db id read-handlers]
  (let [resp (fire/read (:db db) (str (:root db) "/data/" id "/meta") (:auth db) {:pool (:pool db)})]
    (deserialize resp read-handlers))) 

(defn update-item [db id data read-handlers binary?]
  (let [serialized (if-not binary?  (serialize id data)  (bserialize id data))
        resp (fire/update! (:db db) (str (:root db)) serialized (:auth db) {:pool (:pool db)})
        item (-> resp :data vals first)]
    [(deserialize (:meta item) read-handlers) (deserialize (:data item) read-handlers)]))

(defn delete-item [db id]
  (let [_ (fire/delete! (:db db) (str (:root db) "/keys/" id) (:auth db) {:async true :pool (:pool db)})
        resp (fire/delete! (:db db) (str (:root db) "/data/" id) (:auth db) {:pool (:pool db)})]
    resp))  

(defn get-keys [db read-handlers]
  (let [resp (fire/read (:db db) (str (:root db) "/keys") (:auth db) {:pool (:pool db)})
        ans (map #(-> (deserialize % read-handlers) :key) (seq (vals resp)))]
    ans))

(defn str-uuid [key]
  (str (hasch/uuid key)))

(defn prep-stream [bytes]
 {:input-stream  (ByteArrayInputStream. bytes):size (count bytes)})

(defn prep-ex [^String message ^Exception e]
  (ex-info message {:error (.getMessage e) :cause (.getCause e) :trace (.getStackTrace e)}))

(defrecord FireStore [state read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (async/put! res-ch (item-exists? state (str-uuid key)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to determine if item exists" e)))))
      res-ch))

  (-get [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-item-only state (str-uuid key) read-handlers)]
            (if (some? res) 
              (async/put! res-ch res)
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value from store" e)))))
      res-ch))

  (-get-meta [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-meta-only state (str-uuid key) read-handlers)]
            (if (some? res) 
              (async/put! res-ch res)
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value metadata from store" e)))))
      res-ch))

  (-update-in [this key-vec meta-up-fn up-fn args]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [binary? false
                [fkey & rkey] key-vec
                old-val (get-item state (str-uuid fkey) read-handlers)
                new-val (update-item 
                              state
                              (str-uuid fkey)
                              (let [[meta data] old-val]
                                [(meta-up-fn meta) (if rkey (apply update-in data rkey up-fn args) (apply up-fn data args))])
                              read-handlers binary?)]
            (async/put! res-ch [(second old-val) (second new-val)]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to update or write value in store" e)))))
        res-ch))

  (-assoc-in [this key-vec meta val] (-update-in this key-vec meta (fn [_] val) []))

  (-dissoc [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (delete-item state (str-uuid key))
          (async/close! res-ch)
          (catch Exception e (async/put! res-ch (prep-ex "Failed to delete key-value pair from store" e)))))
        res-ch))

  PBinaryAsyncKeyValueStore
  (-bget [this key locked-cb]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-item-only state (str-uuid key) read-handlers)]
            (if (some? res) 
              (async/put! res-ch (locked-cb (prep-stream res)))  
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value from store" e)))))
      res-ch))

  (-bassoc [this key meta-up-fn input]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [binary? true
                old-val (get-item state (str-uuid key) read-handlers)
                new-val (update-item 
                              state
                              (str-uuid key)
                              (let [[meta _] old-val] ;We ignore the existing binary data and overwrite it.
                                [(meta-up-fn meta) input])
                              read-handlers binary?)]
            (async/put! res-ch [(second old-val) (second new-val)]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to update or write value in store" e)))))
        res-ch))

  PKeyIterable
  (-keys [_]
   (let [res-ch (async/chan)]
      (async/thread
        (try
          (doall
            (map 
              #(async/put! res-ch %)
              (get-keys state read-handlers)))
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
                final-db (if (nil? db) (:project-id auth) db)]
            (async/put! res-ch
              (map->FireStore { :state {:db final-db :auth auth :root root}
                                :serializer (ser/string-serializer)
                                :read-handlers read-handlers
                                :write-handlers write-handlers
                                :locks (atom {})})))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to connect to store" e)))))          
        res-ch))

(defn delete-store [store]
  (let [res-ch (async/chan 1)]
    (async/thread
      (try
        (let [state (:state store)]
          (fire/delete! (:db state) (str (:root state)) (:auth state)))
        (catch Exception e (async/put! res-ch (prep-ex "Failed to delete store" e)))))          
        res-ch))

