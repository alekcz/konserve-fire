(ns konserve-fire.io
  "IO function for interacting with database"
  (:require [fire.core :as fire]
            [clojure.string :as str])
  (:import  [java.util Base64 Base64$Decoder Base64$Encoder]
            [java.io ByteArrayInputStream]))

(set! *warn-on-reflection* 1)

(def ^Base64$Encoder b64encoder (. Base64 getEncoder))
(def ^Base64$Decoder b64decoder (. Base64 getDecoder))

(defn chunk-str [string]
  (when string
     (->> string
          (re-seq #".{1,5000000}") 
          (map-indexed (fn [idx itm] {(str "p" idx) (str/join itm)}))
          (apply merge {}))))

(defn combine-str [data-str]
  (when data-str
    (->> (dissoc data-str :headers) (into (sorted-map)) vals str/join)))

(defn split-header [bytes]
  (when bytes
    (let [data  (->> bytes vec (split-at 4))
          streamer (fn [header data] (list (byte-array header) (-> data byte-array (ByteArrayInputStream.))))]
      (apply streamer data))))

(defn prep-write 
  [data]
  (let [[meta val] data]
    {:meta  (when meta 
              (chunk-str (.encodeToString b64encoder ^"[B" meta)))
     :data  (when val 
              (chunk-str (.encodeToString b64encoder ^"[B"  val)))}))

(defn prep-read 
  [data']
  (let [meta (combine-str (:meta data'))
        data (combine-str (:data data'))]
    [ (when meta 
        (split-header (.decode b64decoder ^String meta))) 
      (when data  
        (split-header (.decode b64decoder ^String data)))]))

(defn guard [data]
  (if-not (:error data)
    data
    (throw (Exception. (str "FIREBASE ERROR:" (:error data))))))

(defn it-exists? 
  [store id]
    (let [resp (guard (fire/read (:db store) (str (:root store) "/" id "/data") (:auth store) {:query {:shallow true}}))]
    (some? resp)))
  
(defn get-it 
  [store id]
  (let [resp (guard (fire/read (:db store) (str (:root store) "/" id) (:auth store)))]
    (prep-read resp)))

(defn get-it-only 
  [store id]
  (let [resp (guard (fire/read (:db store) (str (:root store) "/" id "/data") (:auth store)))]
    (when resp (->> resp ^String (combine-str) (.decode b64decoder) split-header))))

(defn get-meta
  [store id]
  (let [resp (guard (fire/read (:db store) (str (:root store) "/" id "/meta") (:auth store)))]
    (when resp (->> resp ^String (combine-str) (.decode b64decoder) split-header))))

(defn update-it 
  [store id data]
  (guard (fire/update! (:db store) (str (:root store) "/" id) (prep-write data) (:auth store) {:print "silent"})))

(defn delete-it 
  [store id]
  (guard (fire/delete! (:db store) (str (:root store) "/" id) (:auth store))))

(defn get-keys 
  [store]
  (let [resp (guard (fire/read (:db store) (str (:root store)) (:auth store) {:query {:shallow true}}))
        key-stream (seq (keys resp))
        getmeta (fn [id] (get-meta store (name id)))]
    (map getmeta key-stream)))

(defn raw-get-it-only 
  [store id]
  (let [resp (guard (fire/read (:db store) (str (:root store) "/" id "/data") (:auth store)))]
    (when resp (->> resp ^String (combine-str) (.decode b64decoder)))))

(defn raw-get-meta 
  [store id]
  (let [resp (guard (fire/read (:db store) (str (:root store) "/" id "/meta") (:auth store)))]
    (when resp (->> resp ^String (combine-str) (.decode b64decoder)))))
  
(defn raw-update-it-only 
  [store id data]
  (when data
    (guard 
      (fire/update! (:db store) (str (:root store) "/" id "/data") 
        (chunk-str (.encodeToString b64encoder ^"[B" data)) (:auth store) {:print "silent"}))))

(defn raw-update-meta
  [store id meta]
  (when meta
    (guard 
      (fire/write! (:db store) (str (:root store) "/" id "/meta") 
        (chunk-str (.encodeToString b64encoder ^"[B" meta)) (:auth store) {:print "silent"}))))
