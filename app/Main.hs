{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Snappy (decompress)

import qualified Data.ByteString as BS

main :: IO ()
main = do
    src <- BS.readFile "./data/Isaac.Newton-Opticks.txt.rawsnappy"
    raw <- BS.readFile "./data/Isaac.Newton-Opticks.txt"
    case decompress src of
        Left e -> print e
        Right decompressed -> do
            print (decompressed == raw)
