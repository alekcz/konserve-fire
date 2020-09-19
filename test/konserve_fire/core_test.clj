(ns konserve-fire.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!!] :as async]
            [konserve.core :as k]
            [konserve.storage-layout :as kl]
            [konserve-fire.core :refer [new-fire-store delete-store]]
            [malli.generator :as mg]
            [fire.auth :as fire-auth]))

(deftype UnknownType [])

(defn exception? [thing]
  (instance? Throwable thing))

(deftest get-nil-tests
  (testing "Test getting on empty store"
    (let [_ (println "Getting from an empty store")
          store (<!! (new-fire-store :fire :root "konserve-nil-test"))]
      (is (= nil (<!! (k/get store :foo))))
      (is (= nil (<!! (k/get-meta store :foo))))
      (is (not (<!! (k/exists? store :foo))))
      (is (= :default (<!! (k/get-in store [:fuu] :default))))
      (<!! (k/bget store :foo (fn [res] 
                                (is (nil? res)))))
      (delete-store store))))

(deftest write-value-tests
  (testing "Test writing to store"
    (let [_ (println "Writing to store")
          auth (fire-auth/create-token :fire)
          db (str "https://" (:project-id auth) ".firebaseio.com")
          store (<!! (new-fire-store :fire :db db :root "konserve-write-test"))]
      (is (not (<!! (k/exists? store :foo))))
      (<!! (k/assoc store :foo :bar))
      (is (<!! (k/exists? store :foo)))
      (is (= :bar (<!! (k/get store :foo))))
      (is (= :foo (:key (<!! (k/get-meta store :foo)))))
      (<!! (k/assoc-in store [:baz] {:bar 42}))
      (is (= 42 (<!! (k/get-in store [:baz :bar]))))
      (delete-store store))))

(deftest update-value-tests
  (testing "Test updating values in the store"
    (let [_ (println "Updating values in the store")
          store (<!! (new-fire-store :fire :root "konserve-updates"))]
      (<!! (k/assoc store :foo :baritone))
      (is (= :baritone (<!! (k/get-in store [:foo]))))
      (<!! (k/update-in store [:foo] name))
      (is (= "baritone" (<!! (k/get-in store [:foo]))))
      (delete-store store))))      

(deftest exists-tests
  (testing "Test check for existing key in the store"
    (let [_ (println "Checking if keys exist")
          store (<!! (new-fire-store :fire :root "konserve-exists-test"))]
      (is (not (<!! (k/exists? store :foo))))
      (<!! (k/assoc store :foo :baritone))
      (is  (<!! (k/exists? store :foo)))
      (<!! (k/dissoc store :foo))
      (is (not (<!! (k/exists? store :foo))))
      (delete-store store))))

(deftest binary-tests
  (testing "Test writing binary date"
    (let [_ (println "Reading and writing binary data")
          store (<!! (new-fire-store :fire :root "konserve-binary-test"))]
      (is (not (<!! (k/exists? store :binbar))))
      (<!! (k/bget store :binbar (fn [ans] (is (nil? ans)))))
      (<!! (k/bassoc store :binbar (byte-array (range 10))))
      (<!! (k/bget store :binbar (fn [{:keys [input-stream]}]
                                    (is (= (map byte (slurp input-stream))
                                           (range 10))))))
      (<!! (k/bassoc store :binbar (byte-array (map inc (range 10))))) 
      (<!! (k/bget store :binbar (fn [{:keys [input-stream]}]
                                    (is (= (map byte (slurp input-stream))
                                           (map inc (range 10)))))))                                          
      (is (<!! (k/exists? store :binbar)))
      (delete-store store))))
  
(deftest key-tests
  (testing "Test getting keys from the store"
    (let [_ (println "Getting keys from store")
          store (<!! (new-fire-store :fire :root "konserve-key-test"))]
      (is (= #{} (<!! (async/into #{} (k/keys store)))))
      (<!! (k/assoc store :baz 20))
      (<!! (k/assoc store :binbar 20))
      (is (= #{:baz :binbar} (<!! (async/into #{} (k/keys store)))))
      (delete-store store))))  

(deftest append-test
  (testing "Test the append store functionality."
    (let [_ (println "Appending to store")
          store (<!! (new-fire-store :fire))]
      (<!! (k/append store :foo {:bar 42}))
      (<!! (k/append store :foo {:bar 43}))
      (is (= (<!! (k/log store :foo))
             '({:bar 42}{:bar 43})))
      (is (= (<!! (k/reduce-log store
                              :foo
                              (fn [acc elem]
                                (conj acc elem))
                              []))
             [{:bar 42} {:bar 43}]))
      (delete-store store))))

(deftest invalid-store-test
  (testing "Invalid store functionality."
    (let [_ (println "Connecting to invalid store")
          store (<!! (new-fire-store :non-existent-key))
          store2 (<!! (new-fire-store :non-existent-key :db "invalid"))]
      (is (exception? store))
      (is (exception? store2)))))

(def home
  [:map
    [:name string?]
    [:description string?]
    [:rooms pos-int?]
    [:capacity float?]
    [:address
      [:map
        [:street string?]
        [:number int?]
        [:country [:enum "kenya" "lesotho" "south-africa" "italy" "mozambique" "spain" "india" "brazil" "usa" "germany"]]]]])

(deftest realistic-test
  (testing "Realistic data test."
    (let [_ (println "Entering realistic data")
          store (<!! (new-fire-store :fire :root "konserve-realistic-test"))
          home (mg/generate home {:size 20 :seed 2})
          address (:address home)
          addressless (dissoc home :address)
          name (mg/generate keyword? {:size 15 :seed 3})
          num1 (mg/generate pos-int? {:size 5 :seed 4})
          num2 (mg/generate pos-int? {:size 5 :seed 5})
          floater (mg/generate float? {:size 5 :seed 6})]
      
      (<!! (k/assoc store name addressless))
      (is (= addressless 
             (<!! (k/get store name))))

      (<!! (k/assoc-in store [name :address] address))
      (is (= home 
             (<!! (k/get store name))))

      (<!! (k/update-in store [name :capacity] * floater))
      (is (= (* floater (:capacity home)) 
             (<!! (k/get-in store [name :capacity]))))  

      (<!! (k/update-in store [name :address :number] + num1 num2))
      (is (= (+ num1 num2 (:number address)) 
             (<!! (k/get-in store [name :address :number]))))             
      
      (delete-store store))))   

(deftest bulk-test
  (testing "Bulk data test."
    (let [_ (println "Writing bulk data")
          store (<!! (new-fire-store :fire :root "konserve-bulk-test"))
          string20MB (apply str (vec (range 3000000)))
          range2MB 2097152
          sevens (repeat range2MB 7)]
      (print "\nWriting 20MB string: ")
      (time (<!! (k/assoc store :record string20MB)))
      (is (= (count string20MB) (count (<!! (k/get store :record)))))
      (print "Writing 2MB binary: ")
      (time (<!! (k/bassoc store :binary (byte-array sevens))))
      (<!! (k/bget store :binary (fn [{:keys [input-stream]}]
                                    (is (= (pmap byte (slurp input-stream))
                                           sevens)))))
      (delete-store store))))  

(deftest raw-meta-test
  (testing "Test header storage"
    (let [_ (println "Checking if headers are stored correctly")
          store (<!! (new-fire-store :fire  :root "konserve-mraw-test"))]
      (<!! (k/assoc store :foo :bar))
      (<!! (k/assoc store :eye :ear))
      (let [mraw (<!! (kl/-get-raw-meta store :foo))
            mraw2 (<!! (kl/-get-raw-meta store :eye))
            mraw3 (<!! (kl/-get-raw-meta store :not-there))
            header (take 4 (map byte mraw))]
        (<!! (kl/-put-raw-meta store :foo mraw2))
        (<!! (kl/-put-raw-meta store :baritone mraw2))
        (is (= header [1 1 1 0]))
        (is (nil? mraw3))
        (is (= :eye (:key (<!! (k/get-meta store :foo)))))
        (is (= :eye (:key (<!! (k/get-meta store :baritone))))))        
      (delete-store store))))          

(deftest raw-value-test
  (testing "Test value storage"
    (let [_ (println "Checking if values are stored correctly")
          store (<!! (new-fire-store :fire :root "konserve-vraw-test"))]
      (<!! (k/assoc store :foo :bar))
      (<!! (k/assoc store :eye :ear))
      (let [mvalue (<!! (kl/-get-raw-value store :foo))
            mvalue2 (<!! (kl/-get-raw-value store :eye))
            mvalue3 (<!! (kl/-get-raw-value store :not-there))
            header (take 4 (map byte mvalue))]
        (<!! (kl/-put-raw-value store :foo mvalue2))
        (<!! (kl/-put-raw-value store :baritone mvalue2))
        (is (= header [1 1 1 0]))
        (is (nil? mvalue3))
        (is (= :ear (<!! (k/get store :foo))))
        (is (= :ear (<!! (k/get store :baritone)))))      
      (delete-store store))))      

(deftest exceptions-test
  (testing "Test exception handling"
    (let [_ (println "Generating exceptions")
          store (<!! (new-fire-store :fire :root "konserve-exceptions-test"))
          params (clojure.core/keys store)
          corruptor (fn [s k] 
                        (if (= (type (k s)) clojure.lang.Atom)
                          (clojure.core/assoc-in s [k] (atom {})) 
                          (clojure.core/assoc-in s [k] (UnknownType.))))
          corrupt (reduce corruptor store params)] ; let's corrupt our store
      (is (exception? (<!! (new-fire-store :not-existent))))
      (is (exception? (<!! (k/get corrupt :bad))))
      (is (exception? (<!! (k/get-meta corrupt :bad))))
      (is (exception? (<!! (k/assoc corrupt :bad 10))))
      (is (exception? (<!! (k/dissoc corrupt :bad))))
      (is (exception? (<!! (k/assoc-in corrupt [:bad :robot] 10))))
      (is (exception? (<!! (k/update-in corrupt [:bad :robot] inc))))
      (is (exception? (<!! (k/exists? corrupt :bad))))
      (is (exception? (<!! (k/keys corrupt))))
      (is (exception? (<!! (k/bget corrupt :bad (fn [_] nil)))))   
      (is (exception? (<!! (k/bassoc corrupt :binbar (byte-array (range 10))))))
      (is (exception? (<!! (kl/-get-raw-value corrupt :bad))))
      (is (exception? (<!! (kl/-put-raw-value corrupt :bad (byte-array (range 10))))))
      (is (exception? (<!! (kl/-get-raw-meta corrupt :bad))))
      (is (exception? (<!! (kl/-put-raw-meta corrupt :bad (byte-array (range 10))))))
      (is (exception? (<!! (delete-store corrupt)))))))