{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}

{-# LANGUAGE Strict #-}

module Snappy (
    compress,
    decompress,
    DecodeError (..),
) where

import Control.Exception (Exception)
import Data.Bits
import Data.ByteString.Internal (ByteString (..))
import Data.Word
import Foreign.ForeignPtr
import Foreign.Marshal.Alloc (callocBytes, free)
import Foreign.Marshal.Utils (copyBytes, fillBytes)
import Foreign.Ptr
import Foreign.Storable
import System.IO.Unsafe (unsafeDupablePerformIO)

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

decompress :: ByteString -> Either DecodeError ByteString
decompress (BS srcFp srcLen)
    | srcLen == 0 = Left EmptyInput
    | otherwise = unsafeDupablePerformIO $
        withForeignPtr srcFp $ \srcPtr -> do
            let srcEnd = srcPtr `plusPtr` srcLen
            vr <- parseVarint srcPtr srcEnd
            case vr of
                Left e -> pure (Left e)
                Right (outLen, hdrLen) -> do
                    let payPtr = srcPtr `plusPtr` hdrLen
                    outFp <- mallocForeignPtrBytes (max 1 outLen)
                    withForeignPtr outFp $ \outPtr -> do
                        r <- decodeLoop payPtr srcEnd outPtr 0 outLen
                        case r of
                            Left e -> pure (Left e)
                            Right _ -> pure (Right (BS outFp outLen))

{-# INLINE parseVarint #-}
parseVarint :: Ptr Word8 -> Ptr Word8 -> IO (Either DecodeError (Int, Int))
parseVarint !ptr !end = go 0 0 0
  where
    go :: Word64 -> Int -> Int -> IO (Either DecodeError (Int, Int))
    go !acc !s !n
        | n > 10 = pure (Left VarIntTooLong)
        | s > 63 = pure (Left Overflow)
        | ptr `plusPtr` n >= end = pure (Left UnexpectedEOF)
        | otherwise = do
            b <- peekByteOff ptr n :: IO Word8
            if b < 0x80
                then pure (Right (fromIntegral (acc .|. (fromIntegral b `unsafeShiftL` s)), n + 1))
                else
                    go
                        (acc .|. ((fromIntegral b .&. 0x7f) `unsafeShiftL` s))
                        (s + 7)
                        (n + 1)

decodeLoop :: Ptr Word8 -> Ptr Word8 -> Ptr Word8 -> Int -> Int -> IO (Either DecodeError Int)
decodeLoop !src0 !srcEnd !dst = go src0
  where
    go :: Ptr Word8 -> Int -> Int -> IO (Either DecodeError Int)
    go !src !di !outLen
        | src >= srcEnd = pure (Right di)
        | otherwise = do
            tag <- peek src :: IO Word8
            let src' = src `plusPtr` 1
            case tag .&. 0x03 of
                0 -> do
                    let hdr = fromIntegral (tag `unsafeShiftR` 2) :: Int
                    if hdr < 60
                        then do
                            let litLen = hdr + 1
                            if src' `plusPtr` litLen > srcEnd || di + litLen > outLen
                                then pure (Left UnexpectedEOF)
                                else do
                                    copyBytes (dst `plusPtr` di) src' litLen
                                    go (src' `plusPtr` litLen) (di + litLen) outLen
                        else do
                            let nb = hdr - 59
                            if nb < 1 || nb > 4
                                then pure (Left UnsupportedLiteralLength)
                                else
                                    if src' `plusPtr` nb > srcEnd
                                        then pure (Left UnexpectedEOF)
                                        else do
                                            lm1 <- readLEn src' nb
                                            let litLen = lm1 + 1
                                                src'' = src' `plusPtr` nb
                                            if src'' `plusPtr` litLen > srcEnd || di + litLen > outLen
                                                then pure (Left UnexpectedEOF)
                                                else do
                                                    copyBytes (dst `plusPtr` di) src'' litLen
                                                    go (src'' `plusPtr` litLen) (di + litLen) outLen
                1 -> do
                    if src' >= srcEnd
                        then pure (Left UnexpectedEOF)
                        else do
                            lo <- peek src' :: IO Word8
                            let cpLen = (fromIntegral ((tag `unsafeShiftR` 2) .&. 0x07) :: Int) + 4
                                offHi = fromIntegral (tag `unsafeShiftR` 5) :: Int
                                offset = (offHi `unsafeShiftL` 8) .|. fromIntegral lo
                            if offset <= 0 || offset > di
                                then pure (Left InvalidOffset)
                                else do
                                    copyOverlap dst di offset cpLen
                                    go (src' `plusPtr` 1) (di + cpLen) outLen
                2 -> do
                    if src' `plusPtr` 2 > srcEnd
                        then pure (Left UnexpectedEOF)
                        else do
                            off <- readLEn src' 2
                            let cpLen = (fromIntegral ((tag `unsafeShiftR` 2) .&. 0x3f) :: Int) + 1
                            if off <= 0 || off > di
                                then pure (Left InvalidOffset)
                                else do
                                    copyOverlap dst di off cpLen
                                    go (src' `plusPtr` 2) (di + cpLen) outLen
                3 -> do
                    if src' `plusPtr` 4 > srcEnd
                        then pure (Left UnexpectedEOF)
                        else do
                            off <- readLEn src' 4
                            let cpLen = (fromIntegral ((tag `unsafeShiftR` 2) .&. 0x3f) :: Int) + 1
                            if off <= 0 || off > di
                                then pure (Left InvalidOffset)
                                else do
                                    copyOverlap dst di off cpLen
                                    go (src' `plusPtr` 4) (di + cpLen) outLen
                _ -> pure (Left InvalidTag)

compress :: ByteString -> ByteString
compress (BS srcFp srcLen) = unsafeDupablePerformIO $
    withForeignPtr srcFp $ \srcPtr -> do
        -- Worst-case expansion: varint header (≤10 B) + per-literal overhead
        let maxOut = 10 + srcLen + (srcLen `div` 6) + 64
        outFp <- mallocForeignPtrBytes maxOut
        withForeignPtr outFp $ \outPtr -> do
            hdrLen <- writeVarint outPtr (fromIntegral srcLen :: Word64)
            written <-
                if srcLen < 4
                    then writeLiteral srcPtr 0 srcLen outPtr hdrLen
                    else do
                        let !tableSize = 1 `unsafeShiftL` 15
                            !tableBytes = tableSize * sizeOfInt
                        tbl <- callocBytes tableBytes :: IO (Ptr Int)
                        w <- compressLoop srcPtr srcLen tbl outPtr hdrLen 0 0
                        free tbl
                        pure w
            pure (BS outFp written)

sizeOfInt :: Int
sizeOfInt = sizeOf (0 :: Int)
{-# INLINE sizeOfInt #-}

compressLoop :: Ptr Word8 -> Int -> Ptr Int -> Ptr Word8 -> Int -> Int -> Int -> IO Int
compressLoop !src !srcLen !tbl !dst !dstOff0 = go dstOff0
  where
    go :: Int -> Int -> Int -> IO Int
    go !dstOff !i !litStart
        | i + 3 >= srcLen = do
            let remLen = srcLen - litStart
            if remLen > 0
                then writeLiteral src litStart remLen dst dstOff
                else pure dstOff
        | otherwise = do
            w <- readU32LE src i
            let !h = hash32 w
            prev0 <- peekByteOff tbl (h * sizeOfInt) :: IO Int
            pokeByteOff tbl (h * sizeOfInt) (i + 1 :: Int)
            let prev = prev0 - 1
            if prev < 0
                then go dstOff (i + 1) litStart
                else do
                    pw <- readU32LE src prev
                    if pw /= w
                        then go dstOff (i + 1) litStart
                        else do
                            dstOff' <-
                                if i > litStart
                                    then writeLiteral src litStart (i - litStart) dst dstOff
                                    else pure dstOff
                            extra <- extendMatchIO src (i + 4) (prev + 4) srcLen
                            let matchLen = 4 + extra
                                offset = i - prev
                            dstOff'' <- writeCopies offset matchLen dst dstOff'
                            let i' = i + matchLen
                            go dstOff'' i' i'

extendMatchIO :: Ptr Word8 -> Int -> Int -> Int -> IO Int
extendMatchIO !ptr !a0 !b0 !limit = go 0
  where
    go !n
        | a0 + n >= limit || b0 + n >= limit = pure n
        | otherwise = do
            ba <- peekByteOff ptr (a0 + n) :: IO Word8
            bb <- peekByteOff ptr (b0 + n) :: IO Word8
            if ba == bb then go (n + 1) else pure n
{-# INLINE extendMatchIO #-}

-- --------------------------------------------------------------------------
-- Helpers – reading
-- --------------------------------------------------------------------------

{-# INLINE readLEn #-}
readLEn :: Ptr Word8 -> Int -> IO Int
readLEn !ptr = \case
    1 -> do
        b0 <- peek ptr :: IO Word8
        pure (fromIntegral b0)
    2 -> do
        b0 <- peek ptr :: IO Word8
        b1 <- peekByteOff ptr 1 :: IO Word8
        pure (fromIntegral b0 .|. (fromIntegral b1 `unsafeShiftL` 8))
    3 -> do
        b0 <- peek ptr :: IO Word8
        b1 <- peekByteOff ptr 1 :: IO Word8
        b2 <- peekByteOff ptr 2 :: IO Word8
        pure
            ( fromIntegral b0
                .|. (fromIntegral b1 `unsafeShiftL` 8)
                .|. (fromIntegral b2 `unsafeShiftL` 16)
            )
    4 -> do
        b0 <- peek ptr :: IO Word8
        b1 <- peekByteOff ptr 1 :: IO Word8
        b2 <- peekByteOff ptr 2 :: IO Word8
        b3 <- peekByteOff ptr 3 :: IO Word8
        pure
            ( fromIntegral b0
                .|. (fromIntegral b1 `unsafeShiftL` 8)
                .|. (fromIntegral b2 `unsafeShiftL` 16)
                .|. (fromIntegral b3 `unsafeShiftL` 24)
            )
    n -> go 0 0 n -- fallback
  where
    go !acc !_ 0 = pure acc
    go !acc !k m = do
        b <- peekByteOff ptr k :: IO Word8
        go (acc .|. (fromIntegral b `unsafeShiftL` (k * 8))) (k + 1) (m - 1)

{-# INLINE readU32LE #-}
readU32LE :: Ptr Word8 -> Int -> IO Word32
readU32LE !ptr !i = do
    b0 <- peekByteOff ptr i :: IO Word8
    b1 <- peekByteOff ptr (i + 1) :: IO Word8
    b2 <- peekByteOff ptr (i + 2) :: IO Word8
    b3 <- peekByteOff ptr (i + 3) :: IO Word8
    pure $!
        fromIntegral b0
            .|. (fromIntegral b1 `unsafeShiftL` 8)
            .|. (fromIntegral b2 `unsafeShiftL` 16)
            .|. (fromIntegral b3 `unsafeShiftL` 24)

{-# INLINE hash32 #-}
hash32 :: Word32 -> Int
hash32 !w =
    let !kMul = 0x1e35a7bd :: Word32
     in fromIntegral ((w * kMul) `unsafeShiftR` (32 - 15))

{-# INLINE writeVarint #-}
writeVarint :: Ptr Word8 -> Word64 -> IO Int
writeVarint !ptr = go 0
  where
    go !off !x
        | x < 0x80 = do
            pokeByteOff ptr off (fromIntegral x :: Word8)
            pure (off + 1)
        | otherwise = do
            pokeByteOff ptr off (fromIntegral (0x80 .|. (x .&. 0x7f)) :: Word8)
            go (off + 1) (x `unsafeShiftR` 7)

writeLiteral :: Ptr Word8 -> Int -> Int -> Ptr Word8 -> Int -> IO Int
writeLiteral _ _ 0 _ !dstOff = pure dstOff
writeLiteral !src !off !len !dst !dstOff
    | len <= 60 = do
        let !tag = fromIntegral (((len - 1) `unsafeShiftL` 2) .|. 0x00) :: Word8
        pokeByteOff dst dstOff tag
        copyBytes (dst `plusPtr` (dstOff + 1)) (src `plusPtr` off) len
        pure (dstOff + 1 + len)
    | otherwise = do
        let l1 = len - 1
            nb = bytesFor l1
            !tag = fromIntegral (((59 + nb) `unsafeShiftL` 2) .|. 0x00) :: Word8
        pokeByteOff dst dstOff tag
        writeLEn dst (dstOff + 1) nb (fromIntegral l1 :: Word32)
        copyBytes (dst `plusPtr` (dstOff + 1 + nb)) (src `plusPtr` off) len
        pure (dstOff + 1 + nb + len)

writeCopies :: Int -> Int -> Ptr Word8 -> Int -> IO Int
writeCopies !offset = go
  where
    go :: Int -> Ptr Word8 -> Int -> IO Int
    go !remLen !dst !dstOff
        | remLen <= 0 = pure dstOff
        | offset <= 0x7FF && remLen >= 4 = do
            let c = min 11 remLen
            dstOff' <- writeCopy1 c offset dst dstOff
            go (remLen - c) dst dstOff'
        | offset <= 0xFFFF = do
            let c = min 64 remLen
            dstOff' <- writeCopy2 c offset dst dstOff
            go (remLen - c) dst dstOff'
        | otherwise = do
            let c = min 64 remLen
            dstOff' <- writeCopy4 c offset dst dstOff
            go (remLen - c) dst dstOff'
{-# INLINE writeCopies #-}

{-# INLINE writeCopy1 #-}
writeCopy1 :: Int -> Int -> Ptr Word8 -> Int -> IO Int
writeCopy1 !len !off !dst !dstOff = do
    let !lenField = (len - 4) .&. 0x7
        !offHi = (off `unsafeShiftR` 8) .&. 0x7
        !tag = fromIntegral ((lenField `unsafeShiftL` 2) .|. (offHi `unsafeShiftL` 5) .|. 0x01) :: Word8
        !lo = fromIntegral (off .&. 0xFF) :: Word8
    pokeByteOff dst dstOff tag
    pokeByteOff dst (dstOff + 1) lo
    pure (dstOff + 2)

{-# INLINE writeCopy2 #-}
writeCopy2 :: Int -> Int -> Ptr Word8 -> Int -> IO Int
writeCopy2 !len !off !dst !dstOff = do
    let !tag = fromIntegral (((len - 1) `unsafeShiftL` 2) .|. 0x02) :: Word8
    pokeByteOff dst dstOff tag
    writeLEn dst (dstOff + 1) 2 (fromIntegral off :: Word32)
    pure (dstOff + 3)

{-# INLINE writeCopy4 #-}
writeCopy4 :: Int -> Int -> Ptr Word8 -> Int -> IO Int
writeCopy4 !len !off !dst !dstOff = do
    let !tag = fromIntegral (((len - 1) `unsafeShiftL` 2) .|. 0x03) :: Word8
    pokeByteOff dst dstOff tag
    writeLEn dst (dstOff + 1) 4 (fromIntegral off :: Word32)
    pure (dstOff + 5)

{-# INLINE writeLEn #-}
writeLEn :: Ptr Word8 -> Int -> Int -> Word32 -> IO ()
writeLEn !dst !off !n !v = go 0
  where
    go !k
        | k >= n = pure ()
        | otherwise = do
            pokeByteOff dst (off + k) (fromIntegral ((v `unsafeShiftR` (8 * k)) .&. 0xFF) :: Word8)
            go (k + 1)

{-# INLINE bytesFor #-}
bytesFor :: Int -> Int
bytesFor x
    | x < 0x100 = 1
    | x < 0x10000 = 2
    | x < 0x1000000 = 3
    | otherwise = 4

{-# INLINE copyOverlap #-}
copyOverlap :: Ptr Word8 -> Int -> Int -> Int -> IO ()
copyOverlap !base !outOff !offset !len
    | offset >= len = copyBytes (base `plusPtr` outOff) (base `plusPtr` srcStart) len
    | offset == 1 = do
        b <- peekByteOff base srcStart :: IO Word8
        fillBytes (base `plusPtr` outOff) b len
    | otherwise = go 0
  where
    !srcStart = outOff - offset
    go !i
        | i >= len = pure ()
        | otherwise = do
            b <- peekByteOff base (srcStart + (i `rem` offset)) :: IO Word8
            pokeByteOff base (outOff + i) b
            go (i + 1)
