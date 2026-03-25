{-# LANGUAGE BangPatterns #-}
module Main where

import Control.Exception (evaluate)
import Data.IORef
import qualified Data.ByteString as BS
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Word (Word8)
import Snappy

main :: IO ()
main = do
    src <- BS.readFile "./data/Isaac.Newton-Opticks.txt"
    let !payloadMB = fromIntegral (BS.length src) / (1024 * 1024) :: Double
        iters = 200 :: Int

    putStrLn $ "Input: " ++ show (BS.length src) ++ " bytes, " ++ show iters ++ " iterations"

    -- Pre-generate varied inputs (16 KB + 1 byte each, tiny overhead)
    let inputs = [ BS.snoc src (fromIntegral i :: Word8) | i <- [1..iters] ]

    -- Compress benchmark
    cRef <- newIORef undefined
    t0c  <- getPOSIXTime
    mapM_ (\tweaked -> do
        let !c = compress tweaked
        _ <- evaluate (BS.length c)
        writeIORef cRef c) inputs
    t1c <- getPOSIXTime
    compressed <- readIORef cRef
    let totalCompMs = realToFrac (t1c - t0c) * 1000 :: Double
        compSecs = realToFrac (t1c - t0c) / fromIntegral iters :: Double
    putStrLn $ "compress: " ++ show (BS.length compressed) ++ " bytes -> "
                ++ showMBps payloadMB compSecs ++ " MB/s"
                ++ "  (total " ++ showMs totalCompMs ++ "ms)"

    -- Pre-compress all inputs so decompress benchmark is decompress-only
    let compInputs = map (compress . BS.snoc src . (fromIntegral :: Int -> Word8)) [1..iters]

    -- Decompress benchmark (decompress only, no compress overhead)
    dRef <- newIORef undefined
    t0d  <- getPOSIXTime
    mapM_ (\c ->
        case decompress c of
            Left e  -> putStrLn ("error: " ++ show e)
            Right !d -> do
                _ <- evaluate (BS.length d)
                writeIORef dRef d) compInputs
    t1d <- getPOSIXTime
    decompResult <- readIORef dRef
    let totalDecompMs = realToFrac (t1d - t0d) * 1000 :: Double
        decompSecs = realToFrac (t1d - t0d) / fromIntegral iters :: Double
    putStrLn $ "decomp:   " ++ show (BS.length decompResult) ++ " bytes -> "
                ++ showMBps payloadMB decompSecs ++ " MB/s"
                ++ "  (total " ++ showMs totalDecompMs ++ "ms)"

showMBps :: Double -> Double -> String
showMBps mb secs = show (round (mb / secs) :: Int)

showMs :: Double -> String
showMs x = show (round x :: Int)
