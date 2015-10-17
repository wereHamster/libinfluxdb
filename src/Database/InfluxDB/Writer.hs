{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Database.InfluxDB.Writer
    ( Config(..), Handle
    , createHandle, newHandle

    , Value(..), Tags, Fields
    , writePoint, writePoint'
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
import           Control.Exception
import           Control.Concurrent
import           Control.Concurrent.STM

import           System.Clock

import           Network.HTTP.Client
import           Network.HTTP.Client.TLS (tlsManagerSettings)

import           Prelude



data Config = Config
    { cURL :: !String
    , cDB  :: !String
    }

data Handle = Handle
    { hPool :: Pool (TQueue (Maybe Point))
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

    return $ Right $ Handle pool

  where
    mkRequestTemplate req = req
        { method      = "POST"
        , path        = "/write"
        , queryString = "?db=" <> T.encodeUtf8 (T.pack (cDB c))
        }


    drainQueue :: TQueue (Maybe Point) -> Int -> Int -> IO [Point]
    drainQueue queue timeout count = do
        box <- newEmptyTMVarIO
        tmp <- newTVarIO []

        void $ forkFinally (go tmp count) $
            (\_ -> atomically $ readTVar tmp >>= putTMVar box . reverse)

        atomically $ readTMVar box

      where
        go _   0 = return ()
        go tmp n = mask $ \restore -> do
            mbPoint <- restore $ atomically $ do
                mbPoint <- readTQueue queue
                case mbPoint of
                    Nothing -> return Nothing
                    Just p  -> do
                        modifyTVar' tmp (\x -> p : x)
                        return mbPoint

            case mbPoint of
                Nothing -> return ()
                Just _  -> do
                    when (n == count) $ do
                        threadId <- myThreadId
                        void $ forkFinally (threadDelay timeout) $ \_ -> killThread threadId

                    restore $ go tmp (n - 1)


    flushQueue :: Request -> TQueue (Maybe Point) -> IO ()
    flushQueue req queue = do
        let batchSize = 50
        let timeout   = 10 * 1000 * 1000

        -- Dequeue the next batch of points. This operation will block until
        -- a batch of points is available.
        points <- drainQueue queue timeout batchSize

        -- If the batch is non-empty, send the points to InfluxDB.
        when (length points > 0) $
            flushPoints manager req points

        -- If the batch is not full, it means there wasn't enough data in the
        -- queue. So we can sleep a bit.
        when (length points < batchSize) $
            threadDelay $ 5 * 1000000

        -- Repeat.
        flushQueue req queue


    allocateResource :: Request -> IO (TQueue (Maybe Point))
    allocateResource req = do
        queue <- newTQueueIO

        -- Fork off a thread which periodically flushes the queue.
        --
        -- Async exceptions in the thread are completely masked, the only way
        -- to dispose of this thread is to write 'Nothing' into the queue.

        void $ mask_ $ forkIO $ flushQueue req queue


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



-- | Send a batch of points to the InfluxDB server. When the server is
-- unreachable, the batch is lost. Delivery is not retried.
flushPoints :: Manager -> Request -> [Point] -> IO ()
flushPoints manager requestTemplate points = do
    void send

  where
    send :: IO (Either SomeException ())
    send = try $ void $ httpLbs req manager

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
