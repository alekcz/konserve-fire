# konserve-fire

A Firebase implementation of the [konserve kv-protocol](https://github.com/replikativ/konserve) on top of Realtime database.

# Status

![master](https://github.com/alekcz/konserve-fire/workflows/master/badge.svg?branch=master) [![codecov](https://codecov.io/gh/alekcz/konserve-fire/branch/master/graph/badge.svg)](https://codecov.io/gh/alekcz/konserve-fire)   

## Prerequisites

For konserve-fire you will need to create a Realtime Database on Firebase and store the service account credentials in the an environment variable. The default variable is `GOOGLE_APPLICATION_CREDENTIALS`. I personally prefer `FIRE`, it's shorter to type.

## Usage

[![Clojars Project](https://img.shields.io/clojars/v/alekcz/konserve-fire.svg)](https://clojars.org/alekcz/konserve-fire)

`[alekcz/konserve-fire "0.4.0"]`

```clojure
(require '[konserve-fire.core :refer :all]
         '[clojure.core.async :refer [<!!] :as async]
         '[konserve.core :as k])
  
  (def fire-store (<!! (new-fire-store "FIRE" :root "/konserve")))

  (<!! (k/exists? fire-store  "cecilia"))
  (<!! (k/get-in fire-store ["cecilia"]))
  (<!! (k/assoc-in fire-store ["cecilia"] 28))
  (<!! (k/update-in fire-store ["cecilia"] inc))
  (<!! (k/get-in fire-store ["cecilia"]))

  (defrecord Test [a])
  (<!! (k/assoc-in fire-store ["agatha"] (Test. 35)))
  (<!! (k/get-in fire-store ["agatha"]))
```

## License

Copyright © 2020 Alexander Oloo

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
