{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}
module Database.SparkT.Client.Websocket ( executeSinglePhrase ) where

import Data.Text (Text)
import Data.String.Conv

import qualified Network.WebSockets  as WS

import Database.SparkT.AST.Protocol
import Database.SparkT.Builder.Scala

single :: (Show a, ToScalaExpr a, Ord a) => Phrase a -> WS.ClientApp ()
single ph conn = do
  putStrLn "Connected!"
  WS.sendTextData conn (toS (toSE ph) :: Text)
  (msg :: Text) <- WS.receiveData conn
  let pong = read (toS msg) :: Response
  print pong
  WS.sendClose conn ("Bye!" :: Text)

executeSinglePhrase ph = WS.runClient "localhost" 36130 "/sparkt/v1" (single ph)
