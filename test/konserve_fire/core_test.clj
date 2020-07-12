(ns konserve-fire.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!!] :as async]
            [konserve.core :as k]
            [konserve-fire.core :refer [new-fire-store delete-store]]
            [malli.generator :as mg]
            [fire.auth :as fire-auth]
            [fire.core :as fire])
  (:import  [clojure.lang ExceptionInfo]))

(deftest get-nil-tests
  (testing "Test getting on empty store"
    (let [_ (println "Getting from an empty store")
          store (<!! (new-fire-store :fire))]
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
          store (<!! (new-fire-store :fire))]
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
          db  (:project-id (fire-auth/create-token :fire))
          store (<!! (new-fire-store :fire :db db))]
      (<!! (k/assoc store :foo :baritone))
      (is (= :baritone (<!! (k/get-in store [:foo]))))
      (<!! (k/update-in store [:foo] name))
      (is (= "baritone" (<!! (k/get-in store [:foo]))))
      (delete-store store))))

(deftest exists-tests
  (testing "Test check for existing key in the store"
    (let [_ (println "Checking if keys exist")
          store (<!! (new-fire-store :fire))]
      (is (not (<!! (k/exists? store :foo))))
      (<!! (k/assoc store :foo :baritone))
      (is  (<!! (k/exists? store :foo)))
      (<!! (k/dissoc store :foo))
      (is (not (<!! (k/exists? store :foo))))
      (delete-store store))))

(deftest binary-tests
  (testing "Test writing binary date"
    (let [_ (println "Reading and writing binary data")
          store (<!! (new-fire-store :fire))]
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
          store (<!! (new-fire-store :fire))]
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
          store (<!! (new-fire-store :non-existent-key))]
      (is (= ExceptionInfo (type store))))))

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
          store (<!! (new-fire-store :fire))
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
          store (<!! (new-fire-store :fire))
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

(deftest exceptions-test
  (testing "Test exception handling"
    (let [_ (println "Generating exceptions")
          store (<!! (new-fire-store :fire))
          corrupt (update-in store [:store] #(dissoc % :auth))] ; let's corrupt our store
      (is (= ExceptionInfo (type (<!! (k/get corrupt :bad)))))
      (is (= ExceptionInfo (type (<!! (k/get-meta corrupt :bad)))))
      (is (= ExceptionInfo (type (<!! (k/assoc corrupt :bad 10)))))
      (is (= ExceptionInfo (type (<!! (k/dissoc corrupt :bad)))))
      (is (= ExceptionInfo (type (<!! (k/assoc-in corrupt [:bad :robot] 10)))))
      (is (= ExceptionInfo (type (<!! (k/update-in corrupt [:bad :robot] inc)))))
      (is (= ExceptionInfo (type (<!! (k/exists? corrupt :bad)))))
      (is (= ExceptionInfo (type (<!! (k/keys corrupt)))))
      (is (= ExceptionInfo (type (<!! (k/bget corrupt :bad (fn [_] nil))))))   
      (is (= ExceptionInfo (type (<!! (k/bassoc corrupt :binbar (byte-array (range 10)))))))   
      (is (= ExceptionInfo (type (<!! (delete-store corrupt)))))
      (delete-store store))))