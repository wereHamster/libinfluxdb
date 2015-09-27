libinfluxdb
===========

A small library for sending metrics data to [InfluxDB][influxdb].


```haskell
import qualified Data.Map as M
import           Database.InfluxDB.Writer

main :: IO ()
main = do
    Right h <- createHandle $ Config "http://localhost:8086" "dbname"

    let tags   = M.empty
    let values = M.singleton "value" (F 2.35)

    writePoint h "cpu_load" tags values
```


[influxdb]: https://influxdb.com/
