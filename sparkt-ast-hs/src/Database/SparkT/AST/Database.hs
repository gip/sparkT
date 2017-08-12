{-# LANGUAGE DeriveGeneric, GADTs, FlexibleContexts, OverloadedStrings,
             UndecidableInstances, TypeSynonymInstances, FlexibleInstances,
             ScopedTypeVariables, DeriveFunctor #-}
module Database.SparkT.AST.Database where

import Data.Typeable
import GHC.Generics

import Database.SparkT.AST.Internal

type TableSchema t = [(String, t, Bool)]
type DatabaseSchema t = (String, [(String, TableSchema t)])

data DDatabaseMappingCtor =
  DDatabaseMapping | DS3 | DPostgresSQL | DRedshift | DParquet |
  DCSV | DAutodetect | DProprietary | DCache
  deriving (Show)

data Storage = S3 | PostgresSQL | Redshift | Cache
  deriving (Eq, Show, Ord, Generic)
instance ToScalaExpr Storage where
  toSE S3 = classCtor DS3 []
  toSE PostgresSQL = classCtor DPostgresSQL []
  toSE Redshift = classCtor DRedshift []
  toSE Cache = classCtor DCache []

data Format = Parquet | CSV String | Autodetect | Proprietary
  deriving (Eq, Show, Ord, Generic)
instance ToScalaExpr Format where
  toSE Parquet = classCtor DParquet []
  toSE (CSV delim) = classCtor DCSV [show delim]
  toSE Autodetect = classCtor DAutodetect []
  toSE Proprietary = classCtor DProprietary []

data Versioned = Versioned {
  batchId :: String,
  revisionId :: Int
} deriving (Eq, Show, Ord, Generic)

-- TODO: rename that, it's not only a database
data DatabaseMapping t = DatabaseMapping {
  table :: String,
  storage :: Storage,
  format :: Format,
  url :: String,
  schema :: DatabaseSchema t
} deriving (Eq, Show, Ord, Generic, Functor)
instance ToScalaExpr t => ToScalaExpr (DatabaseMapping t) where
  toSE (DatabaseMapping table sto fmt url schema) =
    classCtor DDatabaseMapping [toSE table, toSE sto, toSE fmt, toSE url, toSE schema]

type DatabaseContext = DatabaseMapping TypeRep
