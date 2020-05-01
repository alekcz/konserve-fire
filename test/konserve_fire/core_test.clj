(ns konserve-fire.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!! <! go chan put! close!] :as async]
            [konserve.core :as k]
            [konserve-fire.core :refer [new-fire-store delete-store]]
            [clojure.java.io :as io]
            [malli.generator :as mg])
  (:import  [clojure.lang ExceptionInfo]
            [java.io IOException]))



(deftest fire-store-test
  (testing "Test the core API."
    (let [store (<!! (new-fire-store "FIRE" :root (str "/konserve-test/at1-" (+ 1 (rand-int 200) (rand-int 1100)))))]
      (is (= (<!! (k/get store :foo))
             nil))
      (time (<!! (k/assoc store :foo :bar)))
      (time(is (= (<!! (k/get store :foo))
             :bar)))
      (<!! (k/assoc-in store [:foo] :bar2))
      (is (= :bar2 (<!! (k/get store :foo))))
      (is (= :default
             (<!! (k/get-in store [:fuu] :default))))
      (is (= :bar2 (<!! (k/get store :foo))))
      (is (= :default
             (<!! (k/get-in store [:fuu] :default))))
      (<!! (k/update-in store [:foo] name))
      (is (= "bar2"
             (<!! (k/get store :foo))))
      (<!! (k/assoc-in store [:baz] {:bar 42}))
      (is (= (<!! (k/get-in store [:baz :bar]))
             42))
      (<!! (k/update-in store [:baz :bar] inc))
      (is (= (<!! (k/get-in store [:baz :bar]))
             43))
      (<!! (k/update-in store [:baz :bar] + 2 3))
      (is (= (<!! (k/get-in store [:baz :bar]))
             48))
      (<!! (k/dissoc store :foo))
      (is (= (<!! (k/get-in store [:foo]))
             nil))
      (<!! (k/assoc-in store [:binbar] {:wishing "I could handle binary files"}))
      (is (= #{:baz :binbar}
             (<!! (async/into #{} (k/keys store)))))
      (delete-store store))))

(deftest append-store-test
  (testing "Test the append store functionality."
    (let [store (<!! (new-fire-store "FIRE" :root (str "/konserve-test/t2-" (+ 1 (rand-int 200) (rand-int 1100)))))]
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
  (testing "Test the append store functionality."
    (let [store (<!! (new-fire-store "DOES_NOT_EXIST"))]
      (is (= IOException (type store))))))

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
    (let [store (<!! (new-fire-store "FIRE" :root (str "/konserve-test/t3-" (+ 1 (rand-int 200) (rand-int 1100)))))
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

(deftest exceptions-test
  (testing "Test the append store functionality."
    (let [store (<!! (new-fire-store "FIRE" :root (str "/konserve-test/t4-" (+ 1 (rand-int 200) (rand-int 1100)))))
          corrupt (assoc-in store [:state :db] "123")]
      (is (= ExceptionInfo (type (<!! (k/get corrupt :bad)))))
      (is (= ExceptionInfo (type (<!! (k/assoc corrupt :bad 10)))))
      (is (= ExceptionInfo (type (<!! (k/dissoc corrupt :bad)))))
      (is (= ExceptionInfo (type (<!! (k/assoc-in corrupt [:bad :robot] 10)))))
      (is (= ExceptionInfo (type (<!! (k/update-in corrupt [:bad :robot] inc)))))
      (is (= ExceptionInfo (type (<!! (k/exists? corrupt :bad)))))
      (delete-store store))))      

(deftest bulk-test
  (testing "Bulk data test."
    (let [store (<!! (new-fire-store "FIRE" :root (str "/konserve-test/bulk-test")))
          h (apply str (vec (take (* 10 1024 1024) (range))))]
      (<!! (k/assoc store :record h))    
      (is (= h (<!! (k/get store :record))))
      (delete-store store))))  

    
      