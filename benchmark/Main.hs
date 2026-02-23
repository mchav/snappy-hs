
{-# LANGUAGE OverloadedStrings #-}


import qualified Codec.Compression.Snappy as SnappyNative
import Control.Exception (evaluate)
import Control.Monad (void)
import Criterion.Main
import qualified Data.ByteString as BS
import qualified Snappy as SnappyHs

snappyHs :: BS.ByteString -> IO ()
snappyHs src = void $ evaluate (BS.length <$> (SnappyHs.decompress . SnappyHs.compress) src)

snappyNative :: BS.ByteString -> IO ()
snappyNative src = void $ evaluate ((BS.length . SnappyNative.decompress . SnappyNative.compress) src)

main = do
    src <- BS.readFile "./data/Isaac.Newton-Opticks.txt"
    defaultMain
        [ bgroup
            "stats"
            [ bench "snappyHs" $ nfIO (snappyHs src)
            , bench "snappyNative" $ nfIO (snappyNative src)
            ]
        ]
