{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import Snappy (decompress)

main :: IO ()
main = do
  let s = decompress "hello"
  print s
