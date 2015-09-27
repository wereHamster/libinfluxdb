{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Database.InfluxDB.Writer
    ( Config, Handle
    , createHandle, newHandle

    , Value(..), Tags, Fields
    , writePoint

    , test
    ) where


import           Data.Int
import           Data.Monoid
import           Data.Pool

import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import           Data.Map (Map)
import qualified Data.Map as M

import           Control.Monad
import           Control.Concurrent
import           Control.Concurrent.STM

import           System.Clock

import           Network.HTTP.Types.Header
import           Network.HTTP.Client
import           Network.HTTP.Client.TLS (tlsManagerSettings)

import           Prelude



data Config = Config
    { cURL :: !String
    , cDB  :: !String
    }

data Handle = Handle
    { hConfig  :: !Config
    , hManager :: !Manager

    , hRequest :: !Request
      -- ^ A template for a request which includes everything but the request
      -- body.

    , hPool :: Pool (TQueue (Maybe Point))
      -- ^ A pool of threads which consume messages from 'TQueue's and write
      -- them in batches to InfluxDB.
    }


createHandle :: Config -> IO (Either () Handle)
createHandle c = do
    manager <- newManager tlsManagerSettings
    newHandle c manager


newHandle :: Config -> Manager -> IO (Either () Handle)
newHandle c manager = do
    req  <- mkRequestTemplate <$> parseUrl (cURL c)
    pool <- createPool (allocateResource req) releaseResource 1 600 3

    return $ Right $ Handle c manager req pool

  where
    mkRequestTemplate req = req
        { method      = "POST"
        , path        = "/write"
        , queryString = "?db=" <> T.encodeUtf8 (T.pack (cDB c))
        }


    dequeuePoints :: [Point] -> TQueue (Maybe Point) -> Int -> STM ([Point], Bool)
    dequeuePoints acc queue n
        | n <= 0    = return $ (reverse acc, False)
        | otherwise = do
            mbPoint <- tryReadTQueue queue
            case mbPoint of
                Nothing       -> return $ (reverse acc, False)
                Just Nothing  -> return $ (reverse acc, True)
                Just (Just p) -> dequeuePoints (p:acc) queue (n - 1)


    dequeueBatch :: [Point] -> TQueue (Maybe Point) -> TimeSpec -> Int -> IO ([Point], Bool)
    dequeueBatch acc queue start n = do
        mbNextBatch <- atomically $ dequeuePoints [] queue n
        case mbNextBatch of
            (points, True) -> return (points, True)
            (points, False) -> do
                -- We can collect more points if we still have space AND time
                -- left. Otherwise return what we have.
                now <- getTime Monotonic
                if length points < n && timeSpecAsNanoSecs (diffTimeSpec start now) < (10 * 1000000000)
                    then dequeueBatch (acc ++ points) queue start (n - length points)
                    else return $ (acc ++ points, False)


    flushQueue :: Manager -> Request -> TQueue (Maybe Point) -> IO ()
    flushQueue manager req queue = do
        -- Dequeue the next batch of points. This operation will block until
        -- 20 points are available or a timeout is reached, whichever comes
        -- first.
        start <- getTime Monotonic
        (points, isLast) <- dequeueBatch [] queue start 20

        let sleep    = threadDelay $ 5 * 1000000
            flush    = flushPoints manager req points
            continue = flushQueue manager req queue

        -- Deciding what to do next is a bit tricky.
        case (length points, isLast) of
            (0, True)  -> return ()
            (0, False) -> sleep >> continue
            (_, True)  -> flush
            (_, False) -> flush >> continue


    allocateResource :: Request -> IO (TQueue (Maybe Point))
    allocateResource req = do
        queue <- newTQueueIO

        -- Fork off a thread which periodically flushes the queue.
        void $ forkIO $ flushQueue manager req queue

        return queue


    releaseResource queue =
        atomically $ writeTQueue queue Nothing




-- | A 'Value' is either an integer, a floating point number, a boolean or
-- string.
data Value = I !Int64 | F !Double | B !Bool | S !Text
    deriving (Show, Eq)

type Tags = Map Text Text
type Fields = Map Text Value

data Point = Point
    { pMeasurement :: !Text
    , pTags :: !Tags
    , pFields :: !Fields
    , pTimestamp :: !(Maybe Int64)
    } deriving (Show, Eq)


-- | Write a point to the database. Generates a timestamp from the local clock.
writePoint :: Handle -> Text -> Tags -> Fields -> IO ()
writePoint h measurement tags fields = do
    timestamp <- fromIntegral . timeSpecAsNanoSecs <$> getTime Realtime
    writePoint' h measurement tags fields timestamp

-- | Same as 'writePoint' but allows the caller to supply the timestamp.
writePoint' :: Handle -> Text -> Tags -> Fields -> Int64 -> IO ()
writePoint' h measurement tags fields timestamp =
    withResource (hPool h) $ \queue -> atomically $ writeTQueue queue $ Just $
        Point measurement tags fields (Just timestamp)



flushPoints :: Manager -> Request -> [Point] -> IO ()
flushPoints manager requestTemplate points = do
    putStrLn $ "Flusing points: " ++ show (length points)
    print body
    void $ httpLbs req manager

  where
    renderValue :: Value -> Text
    renderValue (I x) = (T.pack $ show x) <> "i"
    renderValue (F x) = (T.pack $ show x)
    renderValue (B x) = (T.pack $ if x then "true" else "false")
    renderValue (S x) = "\"" <> x <> "\""

    tagsList :: Tags -> [Text]
    tagsList tags = map (\(k,v) -> k <> "=" <> v) $ M.toList tags
    fieldsList fields = map (\(k,v) -> k <> "=" <> renderValue v) $ M.toList fields

    line :: Point -> Text
    line Point{..} = mconcat $
        [ T.intercalate "," ([pMeasurement] ++ (tagsList pTags))
        , " "
        , T.intercalate "," (fieldsList pFields)
        ] ++ maybe [] (\x -> [" ", T.pack $ show x]) pTimestamp

    body = T.encodeUtf8 $ T.intercalate "\n" $ map line points
    req = requestTemplate { requestBody = RequestBodyBS body }


test = do
    Right h <- createHandle (Config "http://localhost:8086/write" "rmx")
    writePoint h "test" (M.empty) ((M.singleton "value" (F 134)))
    threadDelay $ 1000000
    writePoint h "test" (M.empty) ((M.singleton "value" (F 234)))
    threadDelay $ 1000000
    writePoint h "test" (M.empty) ((M.singleton "value" (F 334)))
