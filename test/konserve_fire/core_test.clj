(ns konserve-fire.core-test
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in dissoc exists? keys])
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!! go] :as async]
            [konserve.core :refer :all]
            [konserve-fire.core :refer [new-fire-store delete-store]]
            [clojure.java.io :as io])
  (:import [clojure.lang ExceptionInfo]))

(deftest fire-store-test
  (testing "Test the core API."
    (let [store (<!! (new-fire-store "alekcz-dev" :env :fire :root (str "/konserve-test/t-" (+ 1 (rand-int 200) (rand-int 1100)))))]
      (is (= (<!! (get-in store [:foo]))
             nil))
      (<!! (assoc store :foo :bar))
      (is (= (<!! (get store :foo))
             :bar))
      (is (true? (<!! (exists? store :foo))))
      (<!! (assoc-in store [:foo] :bar2))
      (is (= :bar2 (<!! (get store :foo))))
      (is (= :default
             (<!! (get-in store [:fuu] :default))))
      (<!! (update store :foo name))
      (is (= "bar2"
             (<!! (get store :foo))))
      (<!! (assoc-in store [:baz] {:bar 42}))
      (is (= (<!! (get-in store [:baz :bar]))
             42))
      (<!! (update-in store [:baz :bar] inc))
      (is (= (<!! (get-in store [:baz :bar]))
             43))
      (<!! (update-in store [:baz :bar] + 2 3))
      (is (= (<!! (get-in store [:baz :bar]))
             48))
      (<!! (dissoc store :foo))
      (is (= (<!! (get-in store [:foo]))
             nil))
      (delete-store store))))

(deftest append-store-test
  (testing "Test the append store functionality."
    (let [store (<!! (new-fire-store "alekcz-dev" :env :fire :root (str "/konserve-test/t-" (+ 1 (rand-int 200) (rand-int 1100)))))]
      (<!! (append store :foo {:bar 42}))
      (<!! (append store :foo {:bar 43}))
      (is (= (<!! (log store :foo))
             '({:bar 42}{:bar 43})))
      (is (= (<!! (reduce-log store
                              :foo
                              (fn [acc elem]
                                (conj acc elem))
                              []))
             [{:bar 42} {:bar 43}]))
      (delete-store store))))

(deftest invalid-store-test
  (testing "Test the append store functionality."
    (let [store (<!! (new-fire-store "alekcz-dev" :env "DOES_NOT_EXIST"))]
      (is (= ExceptionInfo (type store))))))
    