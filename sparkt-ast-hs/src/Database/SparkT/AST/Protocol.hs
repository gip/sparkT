{-# LANGUAGE DeriveGeneric, GADTs, FlexibleContexts, OverloadedStrings,
             UndecidableInstances, TypeSynonymInstances, FlexibleInstances,
             ScopedTypeVariables #-}
module Database.SparkT.AST.Protocol where

import Database.SparkT.AST.SQL
import Database.SparkT.AST.ETL
import Database.SparkT.Builder.Scala

data PhraseCtor = PPing | PSQLInsertStatement | PSQLSelectStatement | PETLStatement
  deriving (Show)

data Phrase a =
    Ping Integer String
  | SQLInsertStatement Integer Bool (Insert a)
  | SQLSelectStatement Integer Bool (Select a)
  | ETLStatement Integer Bool (ETL a)
  deriving (Show)
instance (Show a, ToScalaExpr (Command a), ToScalaExpr a, Ord a) => ToScalaExpr (Phrase a) where
  toSE (SQLInsertStatement id_ exe ins) = classCtor PSQLInsertStatement [toSE id_, toSE exe, toSE ins]
  toSE (SQLSelectStatement id_ exe sel) = classCtor PSQLSelectStatement [toSE id_, toSE exe, toSE sel]
  toSE (ETLStatement id_ exe dag) = classCtor PETLStatement [toSE id_, toSE exe, toSE dag]
  toSE (Ping id_ msg) = classCtor PPing [toSE id_, toSE msg]

data Response =
    Pong Integer String
  | SQLResult Integer (Either String String)
  | UnhandledException Integer String
  | Unparseable (Maybe String)
  | Unsupported Integer String
  | ETLResult Integer (Either String String)
  deriving (Show, Read)
