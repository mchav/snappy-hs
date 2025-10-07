{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE Strict #-}

module Snappy (
    compress,
    decompress,
    DecodeError (..),
) where

import Control.Exception (Exception)
import Control.Monad.ST (ST, runST)
import Data.Bits ((.&.), (.<<.), (.>>.), (.|.))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word

decompress :: BS.ByteString -> Either DecodeError BS.ByteString
decompress s = do
    (_, bytesRead) <- decodedLength s
    let payload = BS.drop bytesRead s
    decode payload

maxVarintLen64 :: Int
maxVarintLen64 = 10

data DecodeError
    = EmptyInput
    | Overflow
    | VarIntTooLong
    | UnexpectedEOF
    | UnsupportedLiteralLength
    | InvalidOffset
    | InvalidTag
    deriving (Show, Eq)
instance Exception DecodeError

decodedLength :: BS.ByteString -> Either DecodeError (Int, Int)
decodedLength src = do
    (value, bytesRead) <- decodeVarInt src
    pure (fromIntegral value, bytesRead)
  where
    decodeVarInt :: BS.ByteString -> Either DecodeError (Word64, Int)
    decodeVarInt bs
        | BS.null bs = Left EmptyInput
        | otherwise = go 0 0 0 bs
      where
        go acc shift bytesRead bytes
            | bytesRead > maxVarintLen64 = Left VarIntTooLong
            | shift > 63 = Left Overflow
            | otherwise = case BS.uncons bytes of
                Nothing -> Left UnexpectedEOF
                Just (b, rest)
                    | b < 0x80 ->
                        Right (acc .|. (fromIntegral b .<<. shift), bytesRead + 1)
                    | otherwise ->
                        go
                            (acc .|. ((fromIntegral b .&. 0x7f) .<<. shift))
                            (shift + 7)
                            (bytesRead + 1)
                            rest

decode :: BS.ByteString -> Either DecodeError BS.ByteString
decode = go mempty BS.empty
  where
    go :: Builder.Builder -> BS.ByteString -> BS.ByteString -> Either DecodeError BS.ByteString
    go !acc !out bs
        | BS.null bs = Right (toStrict acc)
        | otherwise = case BS.uncons bs of
            Nothing -> Left UnexpectedEOF
            Just (tag, rest) ->
                case tag .&. 0x03 of
                    0 -> do
                        let hdr = fromIntegral (tag .>>. 2) :: Int
                        if hdr < 60
                            then do
                                let len = hdr + 1
                                (lit, rest') <- takeN len rest
                                let acc' = acc <> Builder.byteString lit
                                    out' = out <> lit
                                go acc' out' rest'
                            else do
                                let nbytes = hdr - 59
                                whenBad (nbytes < 1 || nbytes > 4) UnsupportedLiteralLength
                                (lenMinus1, rest1) <- readLE nbytes rest
                                let len = lenMinus1 + 1
                                (lit, rest') <- takeN len rest1
                                let acc' = acc <> Builder.byteString lit
                                    out' = out <> lit
                                go acc' out' rest'
                    1 -> do
                        (lo, rest1) <- take1 rest
                        let len = (fromIntegral ((tag .>>. 2) .&. 0x07) :: Int) + 4
                            offHi = fromIntegral (tag .>>. 5) :: Int
                            offset = (offHi .<<. 8) .|. fromIntegral lo
                        chunk <- makeCopy out offset len
                        let acc' = acc <> Builder.byteString chunk
                            out' = out <> chunk
                        go acc' out' rest1
                    2 -> do
                        (off, rest1) <- readLE 2 rest
                        let len = (fromIntegral ((tag .>>. 2) .&. 0x3f) :: Int) + 1
                        chunk <- makeCopy out off len
                        let acc' = acc <> Builder.byteString chunk
                            out' = out <> chunk
                        go acc' out' rest1
                    3 -> do
                        (off, rest1) <- readLE 4 rest
                        let len = (fromIntegral ((tag .>>. 2) .&. 0x3f) :: Int) + 1
                        chunk <- makeCopy out off len
                        let acc' = acc <> Builder.byteString chunk
                            out' = out <> chunk
                        go acc' out' rest1
                    _ -> Left InvalidTag

toStrict :: Builder.Builder -> BS.ByteString
toStrict = LBS.toStrict . Builder.toLazyByteString

take1 :: BS.ByteString -> Either DecodeError (Word8, BS.ByteString)
take1 bs = maybe (Left UnexpectedEOF) Right (BS.uncons bs)

takeN :: Int -> BS.ByteString -> Either DecodeError (BS.ByteString, BS.ByteString)
takeN n bs
    | BS.length bs < n = Left UnexpectedEOF
    | otherwise = Right (BS.take n bs, BS.drop n bs)

readLE :: Int -> BS.ByteString -> Either DecodeError (Int, BS.ByteString)
readLE n bs = do
    (bytes, rest) <- takeN n bs
    let val = leBytesToInt bytes
    pure (val, rest)

leBytesToInt :: BS.ByteString -> Int
leBytesToInt =
    snd . BS.foldl' (\(!i, !acc) b -> (i + 8, acc .|. (fromIntegral b .<<. i))) (0, 0)

whenBad :: Bool -> DecodeError -> Either DecodeError ()
whenBad True e = Left e
whenBad False _ = Right ()

makeCopy :: BS.ByteString -> Int -> Int -> Either DecodeError BS.ByteString
makeCopy out offset len = do
    whenBad (offset <= 0) InvalidOffset
    let outLen = BS.length out
    whenBad (offset > outLen) InvalidOffset
    let start = outLen - offset
        pattern = BS.take offset (BS.drop start out)
    pure $ repeatToLen pattern len

repeatToLen :: BS.ByteString -> Int -> BS.ByteString
repeatToLen pat n =
    let plen = BS.length pat
        times = n `div` plen
        remain = n `mod` plen
        bld =
            mconcat (replicate times (Builder.byteString pat))
                <> Builder.byteString (BS.take remain pat)
     in LBS.toStrict (Builder.toLazyByteString bld)

compress :: BS.ByteString -> BS.ByteString
compress src =
    let header = Builder.toLazyByteString (putVarint (fromIntegral (BS.length src)))
        payload = Builder.toLazyByteString (encodePayload src)
     in LBS.toStrict (header <> payload)

encodePayload :: BS.ByteString -> Builder.Builder
encodePayload bs
    | n < 4 = emitLiteral bs 0 n
    | otherwise = runST $ do
        let tableSize = 1 .<<. 15
        tbl <- VUM.replicate tableSize (-1 :: Int)
        loop tbl 0 0 mempty
  where
    n = BS.length bs

    loop :: VUM.MVector s Int -> Int -> Int -> Builder.Builder -> ST s Builder.Builder
    loop tbl !i !litStart !acc
        | i + 3 >= n = pure (acc <> emitLiteral bs litStart (n - litStart))
        | otherwise = do
            let !w = readU32LE bs i
                !h = hash32 w
            prev <- VUM.read tbl h
            VUM.write tbl h i
            case prev of
                (-1) -> loop tbl (i + 1) litStart acc
                p -> do
                    let off = i - p
                    if off <= 0 || p + 3 >= n
                        then loop tbl (i + 1) litStart acc
                        else
                            if readU32LE bs p == w
                                then do
                                    let m0 = 4 + extendMatch bs (i + 4) (p + 4) n
                                        acc' =
                                            if i > litStart
                                                then acc <> emitLiteral bs litStart (i - litStart)
                                                else acc
                                        acc'' = acc' <> emitCopies off m0
                                        i' = i + m0
                                    loop tbl i' i' acc''
                                else loop tbl (i + 1) litStart acc

extendMatch :: BS.ByteString -> Int -> Int -> Int -> Int
extendMatch bs = go
  where
    go !a !b !n
        | a < n && b < n && BS.index bs a == BS.index bs b = 1 + go (a + 1) (b + 1) n
        | otherwise = 0

hash32 :: Word32 -> Int
hash32 w =
    let kMul :: Word32
        kMul = 0x1e35a7bd
        bucketBits = 15
     in fromIntegral ((w * kMul) .>>. (32 - bucketBits))

readU32LE :: BS.ByteString -> Int -> Word32
readU32LE s i =
    let b0 = fromIntegral (BS.index s i) :: Word32
        b1 = fromIntegral (BS.index s (i + 1)) :: Word32
        b2 = fromIntegral (BS.index s (i + 2)) :: Word32
        b3 = fromIntegral (BS.index s (i + 3)) :: Word32
     in b0 .|. (b1 .<<. 8) .|. (b2 .<<. 16) .|. (b3 .<<. 24)

putVarint :: Word64 -> Builder.Builder
putVarint x0 = go x0
  where
    go !x
        | x < 0x80 = Builder.word8 (fromIntegral x)
        | otherwise =
            Builder.word8 (fromIntegral (0x80 .|. (x .&. 0x7f)))
                <> go (x .>>. 7)

emitLiteral :: BS.ByteString -> Int -> Int -> Builder.Builder
emitLiteral _ _ 0 = mempty
emitLiteral bs off len
    | len <= 60 =
        let tag = fromIntegral (((len - 1) .<<. 2) .|. 0x00) :: Word8
         in Builder.word8 tag <> Builder.byteString (bsSlice bs off len)
    | otherwise =
        let l1 = len - 1
            nbytes = bytesFor l1
            tag = fromIntegral (((59 + nbytes) .<<. 2) .|. 0x00) :: Word8
         in Builder.word8 tag
                <> putLE nbytes (fromIntegral l1 :: Word32)
                <> Builder.byteString (bsSlice bs off len)

emitCopies :: Int -> Int -> Builder.Builder
emitCopies !offset !len
    | len <= 0 = mempty
    | otherwise =
        let (chunk, rest) =
                if offset <= 0x7FF && len >= 4
                    then
                        let c = min 11 len
                         in (emitCopy1 c offset, len - c)
                    else
                        if offset <= 0xFFFF
                            then
                                let c = min 64 len
                                 in (emitCopy2 c offset, len - c)
                            else
                                let c = min 64 len
                                 in (emitCopy4 c offset, len - c)
         in chunk <> emitCopies offset rest

emitCopy1 :: Int -> Int -> Builder.Builder
emitCopy1 len off =
    let lenField = (len - 4) .&. 0x7
        offHi = (off .>>. 8) .&. 0x7
        tag = fromIntegral ((lenField .<<. 2) .|. (offHi .<<. 5) .|. 0x01) :: Word8
        lo = fromIntegral (off .&. 0xFF) :: Word8
     in Builder.word8 tag <> Builder.word8 lo

emitCopy2 :: Int -> Int -> Builder.Builder
emitCopy2 len off =
    let tag = fromIntegral (((len - 1) .<<. 2) .|. 0x02) :: Word8
     in Builder.word8 tag
            <> putLE 2 (fromIntegral off :: Word32)

emitCopy4 :: Int -> Int -> Builder.Builder
emitCopy4 len off =
    let tag = fromIntegral (((len - 1) .<<. 2) .|. 0x03) :: Word8
     in Builder.word8 tag
            <> putLE 4 (fromIntegral off :: Word32)

putLE :: Int -> Word32 -> Builder.Builder
putLE n v = mconcat [Builder.word8 (fromIntegral ((v .>>. (8 * k)) .&. 0xFF)) | k <- [0 .. n - 1]]

bytesFor :: Int -> Int
bytesFor x
    | x < 0x100 = 1
    | x < 0x10000 = 2
    | x < 0x1000000 = 3
    | otherwise = 4

bsSlice :: BS.ByteString -> Int -> Int -> BS.ByteString
bsSlice s off len = BS.take len (BS.drop off s)
