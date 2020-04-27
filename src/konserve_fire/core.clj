(ns konserve-fire.core
  "Address globally aggregated immutable key-value store(s)."
  (:require [clojure.core.async :refer [go to-chan]]
            [konserve.serializers :as ser]
            [hasch.core :as hasch]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get -get-meta
                                        -update-in -assoc-in -dissoc
                                        PKeyIterable
                                        -keys]]
            [clojure.core.async :as async]
            [incognito.edn :refer [read-string-safe]]
            [fire.auth :as fire-auth]
            [fire.core :as fire]))

(set! *warn-on-reflection* 1)

(def maxi (* 9.5 1024 1024))

(defn serialize [data]
  (let [data' (pr-str data)
        size (count data')]
    (if (< size maxi)
      {:kfd data'}
      (throw (Exception. (str "Maximum value size exceeded!: " size))))))

(defn deserialize [data' read-handlers]
   (read-string-safe @read-handlers (:kfd data')))

(defn item-exists? [db id]
  (let [resp (fire/read (:db db) (str (:root db) "/keys/" id) (:auth db) {:shallow true})]
    (some? resp)))

(defn get-item [db id read-handlers]
  (let [resp (fire/read (:db db) (str (:root db) "/data/" id) (:auth db))]
    (deserialize resp read-handlers)))

(defn get-item-meta [db id read-handlers]
  (let [resp (fire/read (:db db) (str (:root db) "/keys/" id) (:auth db))]
    (read-string-safe @read-handlers resp)))

(defn update-item [db id data read-handlers]
  (let [resp (fire/update! (:db db) (str (:root db) "/data/" id) (serialize data) (:auth db))
        _ (fire/update! (:db db) (str (:root db) "/keys/" id) {:key (-> data first pr-str)} (:auth db))]
    (deserialize resp read-handlers)))

(defn delete-item [db id]
  (let [_ (fire/delete! (:db db) (str (:root db) "/keys/" id) (:auth db))
        resp (fire/delete! (:db db) (str (:root db) "/data/" id) (:auth db))]
    resp))  

(defn get-keys [db]
  (let [resp (fire/read (:db db) (str (:root db) "/keys") (:auth db))
        extract (fn [k] (:key (read-string-safe {} k)))]
    (map #(-> % :key extract) (vals resp))))

(defn uuid [key]
  (str (hasch/uuid key)))

(defrecord FireStore [state read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? [this key] (go (if (item-exists? state (uuid key)) true false)))
  (-get [this key] (go (second (get-item state (uuid key) read-handlers))))
  (-get-meta [this key] (go (get-item-meta state (uuid key))))
  (-update-in [this key-vec meta-up-fn up-fn args]
    (go
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
        [(second old-val)
         (second updated-val)])))
  (-assoc-in [this key-vec meta val] (-update-in this key-vec meta (fn [_] val) []))
  (-dissoc [this key] (go (delete-item state (uuid key)) nil))
  
  PKeyIterable
  (-keys [_]
    (to-chan (get-keys state))))

(defn new-fire-store
  "Creates an new store based on Firebase's realtime database."
  [env & {:keys [root read-handlers write-handlers]
          :or  {root "/konserve-fire"
                read-handlers (atom {})
                write-handlers (atom {})}}]
    (let [res-ch (async/chan)] 
      (try
        (let [auth (fire-auth/create-token env)]
          (async/put! res-ch 
            (map->FireStore { :state {:db (:project-id auth) :auth auth :root root}
                              :serializer (ser/string-serializer)
                              :read-handlers read-handlers
                              :write-handlers write-handlers
                              :locks (atom {})})))
        (catch Exception e
          (async/put! res-ch (ex-info "Could note connect to Realtime database." {:type :db-error :state env :exception e}))))
      res-ch))

(defn delete-store [store]
  (let [state (:state store)]
    (fire/delete! (:db state) (str (:root state)) (:auth state))))
