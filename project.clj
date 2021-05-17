(defproject alekcz/konserve-fire "0.3.0-alpha2"
  :description "A Firebase backend for konserve."
  :url "https://github.com/alekcz/konserve-fire"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :aot :all
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [alekcz/fire "0.3.1"]
                 [io.replikativ/konserve "0.6.0-alpha3"]]
  :repl-options {:init-ns konserve-fire.core}
  :plugins [[lein-cloverage "1.2.0"]]
  :profiles { :dev {:dependencies [[metosin/malli "0.0.1-20200404.091302-14"]]}})
