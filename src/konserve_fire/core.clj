(ns konserve-fire.core
  "Address globally aggregated immutable key-value store(s)."
  (:require [clojure.core.async :refer [thread chan onto-chan ] :as async]
            [konserve.serializers :as ser]
            [hasch.core :as hasch]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get -get-meta
                                        -update-in -assoc-in -dissoc
                                        PKeyIterable
                                        -keys]]
            [konserve.utils :refer [go-try <? throw-if-exception]]
            [incognito.edn :refer [read-string-safe]]
            [fire.auth :as fire-auth]
            [fire.core :as fire]
            [clojure.string :as str]))

(set! *warn-on-reflection* 1)

(defn serialize [data]
  (let [data' {:kfd (str/split (pr-str data) #"(?<=\G.{5000000})")}]
    data'))

(defn deserialize [data' read-handlers]
  (if (nil? data')
     nil
    (read-string-safe @read-handlers (apply str (:kfd data')))))

(defn item-exists? [db id]
  (let [resp (fire/read (:db db) (str (:root db) "/data/" id) (:auth db) {:query {:shallow true} :pool (:pool db)})]
    (some? resp)))

(defn get-item [db id read-handlers]
  (let [resp (fire/read (:db db) (str (:root db) "/data/" id) (:auth db) {:pool (:pool db)})]
    (deserialize resp read-handlers)))

(defn update-item [db id data read-handlers]
  (let [resp (fire/update! (:db db) (str (:root db) "/data/" id) (serialize data) (:auth db) {:pool (:pool db)})]
    (deserialize resp read-handlers)))

(defn delete-item [db id]
  (let [resp (fire/delete! (:db db) (str (:root db) "/data/" id) (:auth db) {:pool (:pool db)})]
    resp))  

(defn get-keys [db read-handlers]
  (let [resp (fire/read (:db db) (str (:root db) "/data") (:auth db) {:query {:shallow false} :pool (:pool db)})
        ans (map #(-> (deserialize % read-handlers) first :key) (seq (vals resp)))]
    ans))

(defn uuid [key]
  (str (hasch/uuid key)))

(defrecord FireStore [state read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? [this key] 
    (let [res-ch (chan 1)]
      (thread
        (try
          (async/put! res-ch (item-exists? state (uuid key)))
          (catch Exception e (async/put! res-ch e))
          (finally (async/close! res-ch))))
      res-ch))

  (-get [this key] 
     (let [res-ch (chan 1)]
      (thread
        (try
          (let [res (get-item state (uuid key) read-handlers)]
            (when (some? res) (async/put! res-ch (second res))))
          (catch Exception e (async/put! res-ch e))
          (finally (async/close! res-ch))))
      res-ch))

  (-get-meta [this key] 
    (let [res-ch (chan 1)]
      (thread
        (try
          (let [res (get-item state (uuid key) read-handlers)]
            (when (some? res) (async/put! res-ch (first res))))
          (catch Exception e (async/put! res-ch e))
          (finally (async/close! res-ch))))
      res-ch))

  (-update-in [this key-vec meta-up-fn up-fn args]
     (let [res-ch (chan 1)]
      (thread
        (try
          (let [[fkey & rkey] key-vec
                old-val (get-item state (uuid fkey) read-handlers)
                updated-val (update-item 
                              state
                              (uuid fkey)
                              (let [[meta data] old-val]
                                [(meta-up-fn meta)
                                  (if rkey
                                    (apply update-in data rkey up-fn args)
                                    (apply up-fn data args))])
                              read-handlers)]
            [(second old-val) (second updated-val)])
        (catch Exception e (async/put! res-ch e))
        (finally (async/close! res-ch))))
        res-ch))

  (-assoc-in [this key-vec meta val] (-update-in this key-vec meta (fn [_] val) []))

  (-dissoc [this key] 
    (let [res-ch (chan 1)]
      (thread
        (try
          (delete-item state (uuid key))
          (catch Exception e (async/put! res-ch e))
          (finally (async/close! res-ch))))
        res-ch))

  PKeyIterable
  (-keys [_]
   (let [res-ch (chan)]
      (thread
        (try
          (doseq [k (get-keys state read-handlers)]
            (async/put! res-ch k))
          (catch Exception e (async/put! res-ch e))
          (finally (async/close! res-ch))))
        res-ch)))

(defn new-fire-store
  "Creates an new store based on Firebase's realtime database."
  [env & {:keys [root read-handlers write-handlers]
          :or  {root "/konserve-fire"
                read-handlers (atom {})
                write-handlers (atom {})}}]
    (let [res-ch (chan 1)]
      (thread
        (try
          (let [auth (fire-auth/create-token env)
                pool (fire/connection-pool 100)]
            (async/put! res-ch
              (map->FireStore { :state {:db (:project-id auth) :auth auth :root root :pool pool}
                                :serializer (ser/string-serializer)
                                :read-handlers read-handlers
                                :write-handlers write-handlers
                                :locks (atom {})})))
          (catch Exception e (async/put! res-ch e))
          (finally (async/close! res-ch))))
        res-ch))

(defn delete-store [store]
  (let [state (:state store)]
    (fire/delete! (:db state) (str (:root state)) (:auth state))))
