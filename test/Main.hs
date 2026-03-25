{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import qualified Data.ByteString as BS
import Snappy

main :: IO ()
main = do
    roundTrip "short repetitive" (BS.replicate 100 65)
    roundTrip "single byte" "x"
    roundTrip "short string" "Hello, world!"
    roundTrip "longer repetitive" (BS.replicate 1000 65)
    roundTrip "varied" "Hello, Snappy! Hello, Snappy! ABCDEFGHIJKLMNOP 1234567890"
    roundTrip "already compressed-like" (BS.pack [0..255])
    putStrLn "All tests passed."

roundTrip :: String -> BS.ByteString -> IO ()
roundTrip label bs =
    case decompress (compress bs) of
        Left e  -> fail $ label ++ ": decode error: " ++ show e
        Right d -> if d == bs
            then putStrLn $ "OK: " ++ label
            else fail $ label ++ ": round-trip mismatch"
