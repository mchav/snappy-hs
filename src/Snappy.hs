{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE MagicHash         #-}
{-# LANGUAGE Strict            #-}
{-# LANGUAGE UnboxedTuples     #-}
{-# LANGUAGE UnliftedNewtypes   #-}

module Snappy (
    compress,
    decompress,
    DecodeError (..),
) where

import Control.Exception (Exception)
import Data.Bits (unsafeShiftL, unsafeShiftR, (.&.), (.|.))
import Data.ByteString.Internal (ByteString (..))
import Data.Word (Word64)
import Foreign.ForeignPtr (mallocForeignPtrBytes, withForeignPtr)
import Foreign.Marshal.Utils (copyBytes, fillBytes)
import Foreign.Ptr (Ptr)
import Foreign.Storable (sizeOf)
import GHC.Exts
    ( Addr#
    , Int (..)
    , MutableByteArray#
    , Ptr (..)
    , RealWorld
    , (+#)
    , eqWord8#
    , indexWord8OffAddr#
    , isTrue#
    , newByteArray#
    , or#
    , plusAddr#
    , readIntArray#
    , readWord8OffAddr#
    , setByteArray#
    , uncheckedShiftL#
    , word8ToWord#
    , wordToWord32#
    , writeIntArray#
    , writeWord8OffAddr#
    )
import GHC.IO (IO (..))
import GHC.Word (Word32 (W32#), Word8 (W8#))
import System.IO.Unsafe (unsafeDupablePerformIO)


decompress :: ByteString -> Either DecodeError ByteString
decompress (BS srcFp srcLen)
    | srcLen == 0 = Left EmptyInput
    | otherwise = unsafeDupablePerformIO $
        withForeignPtr srcFp $ \(Ptr srcAddr#) -> do
            vr <- parseVarint srcAddr# srcLen
            case vr of
                Left e -> pure (Left e)
                Right (outLen, hdrLen) -> do
                    outFp <- mallocForeignPtrBytes (max 1 outLen)
                    withForeignPtr outFp $ \(Ptr outAddr#) -> do
                        r <- decodeLoop srcAddr# hdrLen srcLen outAddr# 0 outLen
                        case r of
                            Left e  -> pure (Left e)
                            Right _ -> pure (Right (BS outFp outLen))

compress :: ByteString -> ByteString
compress (BS srcFp srcLen) = unsafeDupablePerformIO $
    withForeignPtr srcFp $ \(Ptr srcAddr#) -> do
        let maxOut = 10 + srcLen + (srcLen `div` 6) + 64
        outFp <- mallocForeignPtrBytes maxOut
        withForeignPtr outFp $ \(Ptr outAddr#) -> do
            hdrLen <- writeVarint outAddr# (fromIntegral srcLen :: Word64)
            written <-
                if srcLen < 4
                    then writeLiteral srcAddr# 0 srcLen outAddr# hdrLen
                    else do
                        let !tableSize  = 1 `unsafeShiftL` 15
                            !tableBytes = tableSize * sizeOfInt
                        tbl <- newZeroedByteArray tableBytes
                        compressLoop srcAddr# srcLen tbl outAddr# hdrLen 0 0
            pure (BS outFp written)


{-# INLINE parseVarint #-}
parseVarint :: Addr# -> Int -> IO (Either DecodeError (Int, Int))
parseVarint !addr# !srcLen = go 0 0 0
  where
    go :: Word64 -> Int -> Int -> IO (Either DecodeError (Int, Int))
    go !acc !s !n
        | n > 10    = pure (Left VarIntTooLong)
        | s > 63    = pure (Left Overflow)
        | n >= srcLen = pure (Left UnexpectedEOF)
        | otherwise =
            let b = fromIntegral (ixByte addr# n) :: Word64
            in if b < 0x80
                then pure (Right (fromIntegral (acc .|. (b `unsafeShiftL` s)), n + 1))
                else go
                    (acc .|. ((b .&. 0x7f) `unsafeShiftL` s))
                    (s + 7)
                    (n + 1)

{-# INLINE writeVarint #-}
writeVarint :: Addr# -> Word64 -> IO Int
writeVarint !addr# = go 0
  where
    go !off !x
        | x < 0x80 = do
            writeW8 addr# off (fromIntegral x :: Word8)
            pure (off + 1)
        | otherwise = do
            writeW8 addr# off (fromIntegral (0x80 .|. (x .&. 0x7f)) :: Word8)
            go (off + 1) (x `unsafeShiftR` 7)

decodeLoop
    :: Addr# -> Int -> Int   -- src base, current offset, end offset
    -> Addr# -> Int -> Int   -- dst base, current offset, capacity
    -> IO (Either DecodeError Int)
decodeLoop !srcAddr# !srcOff0 !srcEnd !outAddr# = go srcOff0
  where
    go :: Int -> Int -> Int -> IO (Either DecodeError Int)
    go !srcOff !outOff !outLen
        | srcOff >= srcEnd = pure (Right outOff)
        | otherwise = do
            let !tag    = ixByte srcAddr# srcOff
                !srcOff1 = srcOff + 1
            case tag .&. 0x03 of
                0 -> do
                    let !hdr = fromIntegral (tag `unsafeShiftR` 2) :: Int
                    if hdr < 60
                        then do
                            let !litLen = hdr + 1
                            if srcOff1 + litLen > srcEnd || outOff + litLen > outLen
                                then pure (Left UnexpectedEOF)
                                else do
                                    copyBytes (atOff outAddr# outOff) (atOff srcAddr# srcOff1) litLen
                                    go (srcOff1 + litLen) (outOff + litLen) outLen
                        else do
                            let !nb = hdr - 59
                            if nb < 1 || nb > 4
                                then pure (Left UnsupportedLiteralLength)
                                else if srcOff1 + nb > srcEnd
                                    then pure (Left UnexpectedEOF)
                                    else do
                                        lm1 <- readLEn srcAddr# srcOff1 nb
                                        let !litLen  = lm1 + 1
                                            !srcOff2 = srcOff1 + nb
                                        if srcOff2 + litLen > srcEnd || outOff + litLen > outLen
                                            then pure (Left UnexpectedEOF)
                                            else do
                                                copyBytes (atOff outAddr# outOff) (atOff srcAddr# srcOff2) litLen
                                                go (srcOff2 + litLen) (outOff + litLen) outLen
                1 -> do
                    if srcOff1 >= srcEnd
                        then pure (Left UnexpectedEOF)
                        else do
                            let !lo    = fromIntegral (ixByte srcAddr# srcOff1) :: Int
                                !cpLen = (fromIntegral ((tag `unsafeShiftR` 2) .&. 0x07) :: Int) + 4
                                !offHi = fromIntegral (tag `unsafeShiftR` 5) :: Int
                                !offset = (offHi `unsafeShiftL` 8) .|. lo
                            if offset <= 0 || offset > outOff
                                then pure (Left InvalidOffset)
                                else do
                                    copyOverlap outAddr# outOff offset cpLen
                                    go (srcOff1 + 1) (outOff + cpLen) outLen
                2 -> do
                    if srcOff1 + 2 > srcEnd
                        then pure (Left UnexpectedEOF)
                        else do
                            off <- readLEn srcAddr# srcOff1 2
                            let !cpLen = (fromIntegral ((tag `unsafeShiftR` 2) .&. 0x3f) :: Int) + 1
                            if off <= 0 || off > outOff
                                then pure (Left InvalidOffset)
                                else do
                                    copyOverlap outAddr# outOff off cpLen
                                    go (srcOff1 + 2) (outOff + cpLen) outLen
                3 -> do
                    if srcOff1 + 4 > srcEnd
                        then pure (Left UnexpectedEOF)
                        else do
                            off <- readLEn srcAddr# srcOff1 4
                            let !cpLen = (fromIntegral ((tag `unsafeShiftR` 2) .&. 0x3f) :: Int) + 1
                            if off <= 0 || off > outOff
                                then pure (Left InvalidOffset)
                                else do
                                    copyOverlap outAddr# outOff off cpLen
                                    go (srcOff1 + 4) (outOff + cpLen) outLen
                _ -> pure (Left InvalidTag)

compressLoop
    :: Addr# -> Int -> HashTable
    -> Addr# -> Int -> Int -> Int
    -> IO Int
compressLoop !src !srcLen !tbl !dst !dstOff0 = go dstOff0
  where
    go :: Int -> Int -> Int -> IO Int
    go !dstOff !i !litStart
        | i + 3 >= srcLen = do
            let !remLen = srcLen - litStart
            if remLen > 0
                then writeLiteral src litStart remLen dst dstOff
                else pure dstOff
        | otherwise = do
            let !w  = readU32LE src i
                !h  = hash32 w
            prev0 <- readTbl tbl h
            writeTbl tbl h (i + 1)
            let !prev = prev0 - 1
            if prev < 0
                then go dstOff (i + 1) litStart
                else do
                    let !pw = readU32LE src prev
                    if pw /= w
                        then go dstOff (i + 1) litStart
                        else do
                            dstOff' <-
                                if i > litStart
                                    then writeLiteral src litStart (i - litStart) dst dstOff
                                    else pure dstOff
                            let !extra    = extendMatch src (i + 4) (prev + 4) srcLen
                                !matchLen = 4 + extra
                                !offset   = i - prev
                            dstOff'' <- writeCopies offset matchLen dst dstOff'
                            let !i' = i + matchLen
                            go dstOff'' i' i'

-- | Extend a match as far as possible; pure since source is immutable.
extendMatch :: Addr# -> Int -> Int -> Int -> Int
extendMatch addr# a0 b0 limit = go 0
  where
    go :: Int -> Int
    go !n
        | a0 + n >= limit || b0 + n >= limit = n
        | otherwise =
            let !(I# an#) = a0 + n
                !(I# bn#) = b0 + n
            in if isTrue# (eqWord8#
                    (indexWord8OffAddr# addr# an#)
                    (indexWord8OffAddr# addr# bn#))
               then go (n + 1)
               else n
{-# INLINE extendMatch #-}


{-# INLINE readLEn #-}
readLEn :: Addr# -> Int -> Int -> IO Int
readLEn !addr# !off !n
    | n == 1 = pure $!
        fromIntegral (ixByte addr# off)
    | n == 2 = pure $!
           fromIntegral (ixByte addr# off)
        .|. (fromIntegral (ixByte addr# (off + 1)) `unsafeShiftL` 8)
    | n == 3 = pure $!
           fromIntegral (ixByte addr# off)
        .|. (fromIntegral (ixByte addr# (off + 1)) `unsafeShiftL` 8)
        .|. (fromIntegral (ixByte addr# (off + 2)) `unsafeShiftL` 16)
    | n == 4 = pure $!
           fromIntegral (ixByte addr# off)
        .|. (fromIntegral (ixByte addr# (off + 1)) `unsafeShiftL` 8)
        .|. (fromIntegral (ixByte addr# (off + 2)) `unsafeShiftL` 16)
        .|. (fromIntegral (ixByte addr# (off + 3)) `unsafeShiftL` 24)
    | otherwise = pure $! go 0 0
  where
    go !acc !k
        | k >= n    = acc
        | otherwise =
            go (acc .|. (fromIntegral (ixByte addr# (off + k)) `unsafeShiftL` (k * 8))) (k + 1)

-- | Read a 32-bit little-endian word; pure since source is immutable.
{-# INLINE readU32LE #-}
readU32LE :: Addr# -> Int -> Word32
readU32LE addr# (I# i#) =
    let b0 = word8ToWord# (indexWord8OffAddr# addr# i#)
        b1 = word8ToWord# (indexWord8OffAddr# addr# (i# +# 1#))
        b2 = word8ToWord# (indexWord8OffAddr# addr# (i# +# 2#))
        b3 = word8ToWord# (indexWord8OffAddr# addr# (i# +# 3#))
        w  = b0 `or#` (b1 `uncheckedShiftL#` 8#)
                `or#` (b2 `uncheckedShiftL#` 16#)
                `or#` (b3 `uncheckedShiftL#` 24#)
    in W32# (wordToWord32# w)

{-# INLINE hash32 #-}
hash32 :: Word32 -> Int
hash32 !w =
    let !kMul = 0x1e35a7bd :: Word32
     in fromIntegral ((w * kMul) `unsafeShiftR` (32 - 15))

writeLiteral :: Addr# -> Int -> Int -> Addr# -> Int -> IO Int
writeLiteral _ _ 0 _ !dstOff = pure dstOff
writeLiteral !src !off !len !dst !dstOff
    | len <= 60 = do
        let !tag = fromIntegral (((len - 1) `unsafeShiftL` 2) .|. 0x00) :: Word8
        writeW8 dst dstOff tag
        copyBytes (atOff dst (dstOff + 1)) (atOff src off) len
        pure (dstOff + 1 + len)
    | otherwise = do
        let !l1  = len - 1
            !nb  = bytesFor l1
            !tag = fromIntegral (((59 + nb) `unsafeShiftL` 2) .|. 0x00) :: Word8
        writeW8 dst dstOff tag
        writeLEn dst (dstOff + 1) nb (fromIntegral l1 :: Word32)
        copyBytes (atOff dst (dstOff + 1 + nb)) (atOff src off) len
        pure (dstOff + 1 + nb + len)

writeCopies :: Int -> Int -> Addr# -> Int -> IO Int
writeCopies !offset = go
  where
    go :: Int -> Addr# -> Int -> IO Int
    go !remLen !dst !dstOff
        | remLen <= 0 = pure dstOff
        | offset <= 0x7FF && remLen >= 4 = do
            let !c = min 11 remLen
            dstOff' <- writeCopy1 c offset dst dstOff
            go (remLen - c) dst dstOff'
        | offset <= 0xFFFF = do
            let !c = min 64 remLen
            dstOff' <- writeCopy2 c offset dst dstOff
            go (remLen - c) dst dstOff'
        | otherwise = do
            let !c = min 64 remLen
            dstOff' <- writeCopy4 c offset dst dstOff
            go (remLen - c) dst dstOff'
{-# INLINE writeCopies #-}

{-# INLINE writeCopy1 #-}
writeCopy1 :: Int -> Int -> Addr# -> Int -> IO Int
writeCopy1 !len !off !dst !dstOff = do
    let !lenField = (len - 4) .&. 0x7
        !offHi    = (off `unsafeShiftR` 8) .&. 0x7
        !tag      = fromIntegral ((lenField `unsafeShiftL` 2) .|. (offHi `unsafeShiftL` 5) .|. 0x01) :: Word8
        !lo       = fromIntegral (off .&. 0xFF) :: Word8
    writeW8 dst dstOff tag
    writeW8 dst (dstOff + 1) lo
    pure (dstOff + 2)

{-# INLINE writeCopy2 #-}
writeCopy2 :: Int -> Int -> Addr# -> Int -> IO Int
writeCopy2 !len !off !dst !dstOff = do
    let !tag = fromIntegral (((len - 1) `unsafeShiftL` 2) .|. 0x02) :: Word8
    writeW8 dst dstOff tag
    writeLEn dst (dstOff + 1) 2 (fromIntegral off :: Word32)
    pure (dstOff + 3)

{-# INLINE writeCopy4 #-}
writeCopy4 :: Int -> Int -> Addr# -> Int -> IO Int
writeCopy4 !len !off !dst !dstOff = do
    let !tag = fromIntegral (((len - 1) `unsafeShiftL` 2) .|. 0x03) :: Word8
    writeW8 dst dstOff tag
    writeLEn dst (dstOff + 1) 4 (fromIntegral off :: Word32)
    pure (dstOff + 5)

{-# INLINE writeLEn #-}
writeLEn :: Addr# -> Int -> Int -> Word32 -> IO ()
writeLEn !dst !off !n !v = go 0
  where
    go !k
        | k >= n    = pure ()
        | otherwise = do
            writeW8 dst (off + k) (fromIntegral ((v `unsafeShiftR` (8 * k)) .&. 0xFF) :: Word8)
            go (k + 1)

{-# INLINE bytesFor #-}
bytesFor :: Int -> Int
bytesFor x
    | x < 0x100     = 1
    | x < 0x10000   = 2
    | x < 0x1000000 = 3
    | otherwise      = 4

{-# INLINE copyOverlap #-}
copyOverlap :: Addr# -> Int -> Int -> Int -> IO ()
copyOverlap !base !outOff !offset !len
    | offset >= len =
        copyBytes (atOff base outOff) (atOff base srcStart) len
    | offset == 1 = do
        b <- readW8 base srcStart
        fillBytes (atOff base outOff) b len
    | otherwise = go 0
  where
    !srcStart = outOff - offset
    go !i
        | i >= len  = pure ()
        | otherwise = do
            b <- readW8 base (srcStart + (i `rem` offset))
            writeW8 base (outOff + i) b
            go (i + 1)

sizeOfInt :: Int
sizeOfInt = sizeOf (0 :: Int)
{-# INLINE sizeOfInt #-}

-- | Lifted wrapper around an unlifted 'MutableByteArray#'.
data HashTable = HashTable (MutableByteArray# RealWorld)

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

-- | Allocate a GC-managed byte array, zero-initialised.
newZeroedByteArray :: Int -> IO HashTable
newZeroedByteArray (I# n#) = IO $ \s0 ->
    case newByteArray# n# s0 of
        (# s1, arr #) -> case setByteArray# arr 0# n# 0# s1 of
            s2 -> (# s2, HashTable arr #)
{-# INLINE newZeroedByteArray #-}

-- | Read the Int at slot index @i@ (each slot is one machine Int).
readTbl :: HashTable -> Int -> IO Int
readTbl (HashTable arr) (I# i#) = IO $ \s ->
    case readIntArray# arr i# s of
        (# s', v# #) -> (# s', I# v# #)
{-# INLINE readTbl #-}

-- | Write an Int to slot index @i@.
writeTbl :: HashTable -> Int -> Int -> IO ()
writeTbl (HashTable arr) (I# i#) (I# v#) = IO $ \s ->
    case writeIntArray# arr i# v# s of
        s' -> (# s', () #)
{-# INLINE writeTbl #-}

-- | Pure read: one byte from an immutable foreign address.
ixByte :: Addr# -> Int -> Word8
ixByte addr# (I# i#) = W8# (indexWord8OffAddr# addr# i#)
{-# INLINE ixByte #-}

-- | Mutable read: one byte from a (possibly mutable) address.
readW8 :: Addr# -> Int -> IO Word8
readW8 addr# (I# i#) = IO $ \s ->
    case readWord8OffAddr# addr# i# s of
        (# s', b# #) -> (# s', W8# b# #)
{-# INLINE readW8 #-}

-- | Write one byte to an address.
writeW8 :: Addr# -> Int -> Word8 -> IO ()
writeW8 addr# (I# i#) (W8# b#) = IO $ \s ->
    case writeWord8OffAddr# addr# i# b# s of
        s' -> (# s', () #)
{-# INLINE writeW8 #-}

-- | Construct a @Ptr@ from a base @Addr#@ plus an @Int@ byte offset.
atOff :: Addr# -> Int -> Ptr a
atOff addr# (I# i#) = Ptr (addr# `plusAddr#` i#)
{-# INLINE atOff #-}
