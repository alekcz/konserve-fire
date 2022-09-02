(defproject alekcz/konserve-fire "0.4.2-SNAPSHOT"
  :description "A Firebase backend for konserve."
  :url "https://github.com/alekcz/konserve-fire"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/core.async "1.5.648"]
                 [alekcz/fire "0.5.1-20220902.150953-8"]
                 [io.replikativ/konserve "0.6.0-alpha3"]]
  :repl-options {:init-ns konserve-fire.core}
  :javac-options ["--release" "8" "-g"]
  :plugins [[lein-cloverage "1.2.2"]]
  :profiles { :dev {:dependencies [[metosin/malli "0.0.1-20200404.091302-14"]
                                   [criterium "0.4.6"]]}})
