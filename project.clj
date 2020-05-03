(defproject alekcz/konserve-fire "0.3.0"
  :description "A Firebase backend for konserve."
  :url "https://github.com/alekcz/konserve-fire"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :aot :all
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [alekcz/fire "0.2.4"]
                 ;[io.replikativ/konserve "0.5.1"]
                 [io.replikativ/incognito "0.2.5"]]
  :repl-options {:init-ns konserve-fire.core}
  :plugins [[lein-cloverage "1.1.2"] [lein-git-deps "0.0.2"]]
  :git-dependencies [["https://github.com/replikativ/konserve.git" "4fac6a9566b2402122f3664aac74d3a08eb36eb3"]]
  :profiles { :dev {:dependencies [[metosin/malli "0.0.1-20200404.091302-14"]]}})
