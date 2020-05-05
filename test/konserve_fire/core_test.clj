(ns konserve-fire.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!!] :as async]
            [konserve.core :as k]
            [konserve-fire.core :refer [new-fire-store delete-store]]
            [malli.generator :as mg]
            [fire.auth :as auth])
  (:import [clojure.lang ExceptionInfo]))

(deftest core-test
  (testing "Test the core API."
    (let [store (<!! (new-fire-store "FIRE" :root (str "/konserve-test-old/t1")))]
      (is (= (<!! (k/get-in store [:foo]))
             nil))
      (is (= (<!! (k/get-in store [:foo nil]))
             nil))
      (<!! (k/bget store :binbar (fn [{:keys [input-stream]}]
                                   (is (nil? input-stream)))))
      (is (false? (<!! (k/exists? store :foo))))
      (<!! (k/assoc store :foo :bar))
      (is (= (<!! (k/get store :foo))
             :bar))
      (is (true? (<!! (k/exists? store :foo))))
      (<!! (k/assoc-in store [:foo] :bar2))
      (is (= :bar2 (<!! (k/get store :foo))))
      (is (= :default
             (<!! (k/get-in store [:fuu] :default))))
      (<!! (k/update store :foo name))
      (is (= "bar2"
             (<!! (k/get store :foo))))
      (<!! (k/assoc-in store [:baz] {:bar 42}))
      (is (= (<!! (k/get-in store [:baz :bar]))
             42))
      (<!! (k/update-in store [:baz :bar] inc))
      (is (= (<!! (k/get-in store [:baz :bar]))
             43))
      (is (= (<!! (k/get-in store [:baz]))
             {:bar 43}))
      (is (= (<!! (k/get-in store [:baz ""]))
             nil))             
      (<!! (k/update-in store [:baz :bar] + 2 3))
      (is (= (<!! (k/get-in store [:baz :bar]))
             48))
      (<!! (k/dissoc store :foo))
      (is (= (<!! (k/get-in store [:foo]))
             nil))
      (<!! (k/bassoc store :binbar (byte-array (range 10))))
      (<!! (k/bget store :binbar (fn [{:keys [input-stream]}]
                                   (is (= (map byte (slurp input-stream))
                                          (range 10))))))
      (delete-store store))))

(deftest append-store-test
  (testing "Test the append store functionality."
    (let [store (<!! (new-fire-store "FIRE" :root (str "/konserve-test-old/t2-" (+ 1 (rand-int 200) (rand-int 1100)))))]
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
    (let [store (<!! (new-fire-store "FIRE" :root (str "/konserve-test-old/t3-" (+ 1 (rand-int 200) (rand-int 1100)))))
          home (mg/generate home {:size 1000 :seed 2})
          address (:address home)
          addressless (dissoc home :address)
          name (mg/generate keyword? {:size 15 :seed 3})
          num1 (mg/generate pos-int? {:size 5 :seed 4})
          num2 (mg/generate pos-int? {:size 5 :seed 5})
          floater (mg/generate float? {:size 5 :seed 6})]
      
      (<!! (k/assoc store name addressless))
      (is (= addressless 
             (<!! (k/get-in store [name]))))

      (<!! (k/assoc-in store [name :address] address))
      (is (= home 
             (<!! (k/get-in store [name]))))

      (<!! (k/update-in store [name :capacity] * floater))
      (is (= (* floater (:capacity home)) 
             (<!! (k/get-in store [name :capacity]))))  

      (<!! (k/update-in store [name :address :number] + num1 num2))
      (is (= (+ num1 num2 (:number address)) 
             (<!! (k/get-in store [name :address :number]))))             
      
      (delete-store store))))     

(deftest bulk-test
  (testing "Bulk data test."
    (let [dbname (:project-id (auth/create-token "FIRE"))
          db (str "https://" dbname ".firebaseio.com")
          store (<!! (new-fire-store "FIRE" :root (str "/konserve-test/bulk-test") :db db))
          string20MB (apply str (vec (range 3000000)))
          range2MB 2097152]
      (print "20MB string: ")
      (time (<!! (k/assoc store :record string20MB)))
      (is (= (count string20MB) (count (<!! (k/get store :record)))))
      (print "2MB binary: ")
      (time (<!! (k/bassoc store :binary (byte-array (repeat range2MB 7)))))
      (<!! (k/bget store :binary (fn [{:keys [input-stream]}]
                                    (is (= (map byte (slurp input-stream))
                                           (repeat range2MB 7))))))
      (delete-store store))))  

    
      