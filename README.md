# snappy-hs

A implementation of the snappy compression algorithm in Haskell.

This implementation is significantly slower than the native C version (or the other Haskell implementations). Use it if you want to decode/encode snappy files cross platform reliably without worrying about C FFI.

There is no plan to optimize this code in the near future but it should be possible given enough time and profiling. So maybe it could be fast in the future but don't hold your breath. Instead, send a PR with some benchmarks.
