# snappy-hs

A pure Haskell implementation of the Snappy compression format.

This library prioritizes portability and simplicity over raw speed. It works reliably across platforms without requiring a C toolchain or dealing with FFI.

## Performance

In informal benchmarks, this implementation is roughly **2–3× slower** than native Snappy.

## When to use

* You want **cross-platform Snappy encode/decode** with a **pure Haskell** dependency stack
* You’d rather avoid **C FFI** and the build/packaging complexity that comes with it

## When not to use

* You need **maximum throughput** (use the native library or an FFI-based binding instead)
