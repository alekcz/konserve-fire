(ns konserve-fire.core
  "Address globally aggregated immutable key-value store(s)."
  (:require [clojure.core.async :refer [go to-chan thread <!!] :as async]
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

(def maxi (* 9.5 1024 1024))

(defn serialize [data]
  (let [data' (pr-str data)
        segments (str/split data' #"(?<=\G.{5000000})")
        size (count data')]
     {:kfd segments}))   
    ; (if (< size maxi)
    ;   {:kfd segments}
    ;   (throw (Exception. (str "Maximum value size exceeded!: " size))))))

(defn deserialize [data' read-handlers]
  (if (nil? data')
    data'
    (read-string-safe @read-handlers (apply str (:kfd data')))))

(defn item-exists? [db id]
  (let [resp (fire/read (:db db) (str (:root db) "/keys/" id) (:auth db) {:query {:shallow true} :pool (:pool db)})]
    (some? resp)))

(defn get-item [db id read-handlers]
  (let [resp (fire/read (:db db) (str (:root db) "/data/" id) (:auth db) {:pool (:pool db)})]
    (deserialize resp read-handlers)))

(defn get-item-meta [db id read-handlers]
  (let [resp (fire/read (:db db) (str (:root db) "/keys/" id) (:auth db) {:pool (:pool db)})]
    (read-string-safe @read-handlers resp)))

(defn update-item [db id data read-handlers]
  (let [resp (fire/update! (:db db) (str (:root db) "/data/" id) (serialize data) (:auth db) {:pool (:pool db)})
        _ (fire/update! (:db db) (str (:root db) "/keys/" id) {:key (-> data first pr-str)} (:auth db) {:pool (:pool db)})]
    (deserialize resp read-handlers)))

(defn delete-item [db id]
  (let [_ (fire/delete! (:db db) (str (:root db) "/keys/" id) (:auth db) {:pool (:pool db)})
        resp (fire/delete! (:db db) (str (:root db) "/data/" id) (:auth db) {:pool (:pool db)})]
    resp))  

(defn get-keys [db]
  (let [resp (fire/read (:db db) (str (:root db) "/keys") (:auth db) {:pool (:pool db)})
        extract (fn [k] (:key (read-string-safe {} k)))]
    (map #(-> % :key extract) (vals resp))))

(defn uuid [key]
  (str (hasch/uuid key)))

(defmacro thread-try
  "Asynchronously executes the body in a go block. Returns a channel
  which will receive the result of the body when completed or the
  exception if an exception is thrown. You are responsible to take
  this exception and deal with it! This means you need to take the
  result from the cannel at some point or the supervisor will take
  care of the error."
  {:style/indent 1}
  [& body]
  (let [[body finally] (if (= (first (last body)) 'finally)
                         [(butlast body) (rest (last body))]
                         [body])]
    `(let [
           ]
       (thread
         (try
           ~@body
           (catch Exception e#
             e#)
           (finally
             ~@finally))))))

(defrecord FireStore [state read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? [this key] (thread-try (if (item-exists? state (uuid key)) true false)))
  (-get [this key] (thread-try (second (get-item state (uuid key) read-handlers))))
  (-get-meta [this key] (thread-try (get-item-meta state (uuid key))))
  (-update-in [this key-vec meta-up-fn up-fn args]
    (thread-try
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
  (-dissoc [this key] (thread-try (delete-item state (uuid key)) nil))
  
  PKeyIterable
  (-keys [_]
    (async/to-chan (get-keys state))))

(defn new-fire-store
  "Creates an new store based on Firebase's realtime database."
  [env & {:keys [root read-handlers write-handlers]
          :or  {root "/konserve-fire"
                read-handlers (atom {})
                write-handlers (atom {})}}]
    (thread-try
      (let [auth (fire-auth/create-token env)
            pool (fire/connection-pool 100)]
        (map->FireStore { :state {:db (:project-id auth) :auth auth :root root :pool pool}
                          :serializer (ser/string-serializer)
                          :read-handlers read-handlers
                          :write-handlers write-handlers
                          :locks (atom {})}))))

(defn delete-store [store]
  (let [state (:state store)]
    (fire/delete! (:db state) (str (:root state)) (:auth state))))
