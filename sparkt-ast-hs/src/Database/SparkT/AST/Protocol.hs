{-# LANGUAGE DeriveGeneric, GADTs, FlexibleContexts, OverloadedStrings,
             UndecidableInstances, TypeSynonymInstances, FlexibleInstances,
             ScopedTypeVariables #-}
module Database.SparkT.AST.Protocol where

import Database.SparkT.AST.Internal
import Database.SparkT.AST.SQL
import Database.SparkT.AST.ETL

data PhraseCtor = PPing | PSQLStatement | PETLStatement
  deriving (Show)

data Phrase a =
    Ping Integer String
  | SQLStatement Integer Bool (Command a)
  | ETLStatement Integer Bool (DAG a)
  deriving (Show)
instance (Show a, ToScalaExpr (Command a), ToScalaExpr a) => ToScalaExpr (Phrase a) where
  toSE (SQLStatement id_ exe sql) = classCtor PSQLStatement [toSE id_, toSE exe, toSE sql]
  toSE (ETLStatement id_ exe dag) = classCtor PETLStatement [toSE id_, toSE exe, toSE dag]
  toSE (Ping id_ msg) = classCtor PPing [toSE id_, toSE msg]

data Response =
    Pong Integer String
  | SQLResult Integer (Either String ())
  | UnhandledException Integer String
  | Unparseable (Maybe String)
  | Unsupported Integer String
  deriving (Show, Read)
