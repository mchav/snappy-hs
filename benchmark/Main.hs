{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import qualified Codec.Compression.Snappy as SnappyNative
import Criterion.Main
import qualified Data.ByteString as BS
import qualified Snappy as SnappyHs

snappyHs :: BS.ByteString -> IO ()
snappyHs src = print (BS.length <$> ((SnappyHs.decompress . SnappyHs.compress) src))

snappyNative :: BS.ByteString -> IO ()
snappyNative src = print ((BS.length . SnappyNative.decompress . SnappyNative.compress) src)

main = do
    src <- BS.readFile "./data/Isaac.Newton-Opticks.txt"
    defaultMain
        [ bgroup
            "stats"
            [ bench "snappyHs" $ nfIO (snappyHs src)
            , bench "snappyNative" $ nfIO (snappyNative src)
            ]
        ]
