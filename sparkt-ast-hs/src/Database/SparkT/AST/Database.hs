{-# LANGUAGE DeriveGeneric, GADTs, FlexibleContexts, OverloadedStrings,
             UndecidableInstances, TypeSynonymInstances, FlexibleInstances,
             ScopedTypeVariables #-}
module Database.SparkT.AST.Database where

import Data.Typeable
import GHC.Generics

import Database.SparkT.AST.Internal

type TableSchema = [(String, TypeRep, Bool)]
type DatabaseSchema = (String, [(String, TableSchema)])

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
data DatabaseMapping = DatabaseMapping {
  table :: String,
  storage :: Storage,
  format :: Format,
  url :: String,
  schema :: DatabaseSchema
} deriving (Eq, Show, Ord, Generic)
instance ToScalaExpr DatabaseMapping where
  toSE (DatabaseMapping table sto fmt url schema) =
    classCtor DDatabaseMapping [toSE table, toSE sto, toSE fmt, toSE url, toSE schema]
