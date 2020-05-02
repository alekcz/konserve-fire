(ns konserve-fire.core
  "Address globally aggregated immutable key-value store(s)."
  (:require [clojure.core.async :as async]
            [konserve.serializers :as ser]
            [hasch.core :as hasch]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get -get-meta
                                        -update-in -assoc-in -dissoc
                                        PKeyIterable
                                        -keys]]
            [incognito.edn :refer [read-string-safe]]
            [fire.auth :as fire-auth]
            [fire.core :as fire]))

(set! *warn-on-reflection* 1)

(def maxi (* 9.5 1024 1024))

(defn serialize [id data]
  (let [serialized (-> data second pr-str)]
    (while (> (count serialized) maxi) 
      (throw (ex-info "Maximum size exceeded" {:cause "Value in kv-pair must be less than 10MB when serialized. 
                                                        See firebase limits."})))
    (let [k {:d (-> data first pr-str)}
          v {:meta {:d (-> data first pr-str)}
             :data {:d (-> data second pr-str)}}]
        {(str "/keys/" id) k (str "/data/" id) v})))

(defn deserialize [data' read-handlers]
  (read-string-safe @read-handlers (:d data')))
    

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

(defn update-item [db id data read-handlers]
  (let [resp (fire/update! (:db db) (str (:root db)) (serialize id data) (:auth db) {:pool (:pool db)})
        item (-> resp :data vals first)]
    [(deserialize (:meta item) read-handlers) (deserialize (:data item) read-handlers)]))

(defn delete-item [db id]
  (let [_ (async/go (fire/delete! (:db db) (str (:root db) "/keys/" id) (:auth db) {:async true :pool (:pool db)}))
        resp (fire/delete! (:db db) (str (:root db) "/data/" id) (:auth db) {:pool (:pool db)})]
    resp))  

(defn get-keys [db read-handlers]
  (let [resp (fire/read (:db db) (str (:root db) "/keys") (:auth db) {:pool (:pool db)})
        ans (map #(-> (deserialize % read-handlers) :key) (seq (vals resp)))]
    ans))

(defn str-uuid [key]
  (str (hasch/uuid key)))

(defn prep-ex [^Exception e]
  (ex-info (.getMessage e) {:cause (.getCause e) :trace (.getStackTrace e)}))

(defrecord FireStore [state read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (async/put! res-ch (item-exists? state (str-uuid key)))
          (catch Exception e (async/put! res-ch (prep-ex e)))))
      res-ch))

  (-get [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-item-only state (str-uuid key) read-handlers)]
            (if (some? res) 
              (async/put! res-ch res)
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex e)))))
      res-ch))

  (-get-meta [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-meta-only state (str-uuid key) read-handlers)]
            (if (some? res) 
              (async/put! res-ch res)
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex e)))))
      res-ch))

  (-update-in [this key-vec meta-up-fn up-fn args]
     (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[fkey & rkey] key-vec
                old-val (get-item state (str-uuid fkey) read-handlers)
                updated-val (update-item 
                              state
                              (str-uuid fkey)
                              (let [[meta data] old-val]
                                [(meta-up-fn meta) (if rkey (apply update-in data rkey up-fn args) (apply up-fn data args))])
                              read-handlers)]
              (async/put! res-ch [(second old-val) (second updated-val)]))
          (catch Exception e (async/put! res-ch (prep-ex e)))))
        res-ch))

  (-assoc-in [this key-vec meta val] (-update-in this key-vec meta (fn [_] val) []))

  (-dissoc [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (delete-item state (str-uuid key))
          (async/close! res-ch)
          (catch Exception e (async/put! res-ch (prep-ex e)))))
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
          (catch Exception e (async/put! res-ch (prep-ex e)))))
        res-ch)))

(defn new-fire-store
  "Creates an new store based on Firebase's realtime database."
  [env & {:keys [root read-handlers write-handlers]
          :or  {root "/konserve-fire"
                read-handlers (atom {})
                write-handlers (atom {})}}]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [auth (fire-auth/create-token env)
                pool (fire/connection-pool 100)]
            (async/put! res-ch
              (map->FireStore { :state {:db (:project-id auth) :auth auth :root root :pool pool}
                                :serializer (ser/string-serializer)
                                :read-handlers read-handlers
                                :write-handlers write-handlers
                                :locks (atom {})})))
          (catch Exception e (async/put! res-ch (prep-ex e)))))          
        res-ch))

(defn delete-store [store]
  (let [res-ch (async/chan 1)]
    (async/thread
      (try
        (let [state (:state store)]
          (fire/delete! (:db state) (str (:root state)) (:auth state)))
        (catch Exception e (async/put! res-ch (prep-ex e)))))          
        res-ch))

