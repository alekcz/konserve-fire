(defproject alekcz/konserve-fire "0.1.0-SNAPSHOT"
  :description "A Firebase backend for konserve."
  :url "https://github.com/alekcz/konserve-fire"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  ;:aot :all
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [io.replikativ/konserve "0.5.1"]
                 [byte-streams "0.2.2"]
                 [alekcz/fire "0.0.2-SNAPSHOT"]]
  :repl-options {:init-ns konserve-fire.core})