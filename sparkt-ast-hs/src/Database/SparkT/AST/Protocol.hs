{-# LANGUAGE DeriveGeneric, GADTs, FlexibleContexts, OverloadedStrings,
             UndecidableInstances, TypeSynonymInstances, FlexibleInstances,
             ScopedTypeVariables #-}
module Database.SparkT.AST.Protocol where

import Database.SparkT.AST.Internal
import Database.SparkT.AST.SQL

data PhraseCtor = PPing | PSQLStatement
  deriving (Show)

data Phrase a =
    Ping Integer String
  | SQLStatement Integer Bool (Command a)
instance (Show a, ToScalaExpr (Command a)) => ToScalaExpr (Phrase a) where
  toSE (SQLStatement id_ exe sql) = classCtor PSQLStatement [toSE id_, toSE exe, toSE sql]
  toSE (Ping id_ msg) = classCtor PPing [toSE id_, toSE msg]

data Response =
    Pong Integer String
  | SQLResult Integer (Either String ())
  | UnhandledException Integer String
  | Unparseable (Maybe String)
  | Unsupported Integer String
  deriving (Show, Read)
