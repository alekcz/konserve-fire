(ns konserve-fire.core
  "Address globally aggregated immutable key-value store(s)."
  (:require [clojure.core.async :refer [go]]
            [konserve.serializers :as ser]
            [hasch.core :refer [uuid]]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get-in -update-in
                                        PBinaryAsyncKeyValueStore
                                        -bassoc -bget]]
            [clojure.core.async :as async]
            [incognito.edn :refer [read-string-safe]]
            [clojure.edn :as edn]
            [fire.auth :as fire-auth]
            [fire.core :as fire]))


(defn serialize [data]
  {:kfd (pr-str data)})

(defn deserialize [data' read-handlers]
   (read-string-safe @read-handlers (:kfd data')))

(defn item-exists? [db id]
  (let [resp (fire/read (:db db) (str (:root db) "/" id) (:auth db) {:shallow true})]
    (true? resp)))

(defn get-item [db id read-handlers]
  (let [resp (fire/read (:db db) (str (:root db) "/" id) (:auth db))]
    (deserialize resp read-handlers)))

(defn update-item [db id data read-handlers]
  (let [resp (fire/update! (:db db) (str (:root db) "/" id) (serialize data) (:auth db))]
    (deserialize resp read-handlers)))

(defn delete-item [db id]
  (let [resp (fire/delete! (:db db) (str (:root db) "/" id) (:auth db))]
    resp))  

(defrecord FireStore [db serializer read-handlers write-handlers locks state]
  PEDNAsyncKeyValueStore
  (-exists? [this key] 
    (let [id (str (uuid key)) res-ch (async/chan)]
      (go 
        (try 
          (async/put! res-ch (item-exists? db id))
          (catch Exception e
            (ex-info "Could not access item" {:type :access-error :id id :key key :exception e}))
          (finally
                (async/close! res-ch))))))

  (-get-in [this key-vec] 
    (let [[fkey & rkey] key-vec 
          id (str (uuid fkey))
          val (get-item db id read-handlers)]
        (if (= val nil)
          (go nil)
          (let [res-ch (async/chan)]
            (try
              (async/put! res-ch (if (empty? rkey) val (get-in val rkey)))
              (catch Exception e
                (async/put! res-ch (ex-info "Could not read key." {:type :read-error :key key-vec :exception e})))
              (finally
                (async/close! res-ch)))
            res-ch)))) 

  (-update-in [this key-vec up-fn] 
    (-update-in this key-vec up-fn []))

  (-update-in [this key-vec up-fn args] 
    (let [[fkey & rkey] key-vec id (str (uuid fkey)) res-ch (async/chan)]
      (try
        (let [old (get-item db id read-handlers)
              new-data (if (empty? rkey) (apply up-fn old args) (apply update-in old rkey up-fn args))
              new (update-item db id new-data read-handlers)]
          (async/put! res-ch [(get-in old rkey) (get-in new rkey)]))
        (catch Exception e
          (async/put! res-ch (ex-info "Could not write key." {:type :write-error :key fkey :exception e})))
        (finally
          (async/close! res-ch)))
      res-ch))

  (-assoc-in [this key-vec val] 
    (-update-in this key-vec (fn [_] val)))
    
  (-dissoc [this key] 
    (let [id (str (uuid key))]
      (go 
        (delete-item db id) 
        nil))))

(defn new-fire-store
  "Creates an new store based on Firebase's realtime database."
  [db & {:keys [env root read-handlers write-handlers]
          :or  {env nil
                root "/konserve-fire"
                read-handlers (atom {})
                write-handlers (atom {})}}]
    (go 
      (try
        (let [auth (fire-auth/create-token env)]
          
          (map->FireStore { :db {:db db :auth auth :root root}
                            :serializer (ser/string-serializer)
                            :read-handlers read-handlers
                            :write-handlers write-handlers
                            :locks (atom {})
                            :state (atom {})}))
        (catch Exception e
          (ex-info "Could note connect to Realtime database." {:type :db-error :db db :exception e})))))

(defn delete-store [db]
  (let [db (:db db)]
    (fire/delete! (:db db) (str (:root db)) (:auth db))))
