{-# LANGUAGE DeriveGeneric, StandaloneDeriving, TypeSynonymInstances,
             FlexibleInstances, TypeFamilies, FlexibleContexts,
             ScopedTypeVariables, PartialTypeSignatures,
             MultiParamTypeClasses, RankNTypes, AllowAmbiguousTypes,
             OverloadedStrings #-}
module Lib where

import Database.Beam
import Database.Beam.Backend.SQL
import Database.SparkT.AST as AST -- This module is defined in this project

import Database.SparkT.AST.Protocol
import Database.SparkT.AST.Database
import Database.SparkT.ETL

import Database.SparkT.Client
import Database.Beam.Backend.SQL.Builder

import Data.Text (Text)
import Data.String.Conv
import Data.Aeson
import qualified Data.ByteString.Lazy.Char8 as BSL (putStrLn)

import Data.Proxy
import Data.Data
import GHC.Generics


-- A table
data ModelDataT f
  = ModelData
  { model_data_sid   :: Columnar f Text
  , model_data_item  :: Columnar f Int
  , model_data_score :: Columnar f Double
  , weird_field      :: Columnar f (Maybe Int)
  } deriving Generic
type ModelData = ModelDataT Identity
type ModelDataId = PrimaryKey ModelDataT Identity
deriving instance Show ModelData
deriving instance Eq ModelData
instance Beamable ModelDataT
instance Table ModelDataT where
  data PrimaryKey ModelDataT f = ModelDataId (Columnar f Text) deriving Generic
  primaryKey = ModelDataId . model_data_sid
instance Beamable (PrimaryKey ModelDataT)


data FirstDb f = FirstDb {
  _firstDbModel     :: f (TableEntity ModelDataT)
} deriving Generic
instance Database FirstDb where
  type InstanceVersioned FirstDb = Versioned
  type InstanceInfo FirstDb = DatabaseMapping
  instanceInfo (dbSettings, Versioned batch rev, tableName) =
    DatabaseMapping (toS tableName) S3 (CSV "|") (concat ["s3://myprefix/", dbName, "/batch_", batch, "/rev_", show rev, "/"]) schema
      where schema@(dbName, _) = dbSchema dbSettings
firstDb :: DatabaseSettings be FirstDb
firstDb = defaultDbSettings

data SecondDb f = SecondDb {
  _secondDbModel     :: f (TableEntity ModelDataT)
} deriving Generic
instance Database SecondDb where
  type InstanceVersioned SecondDb = Versioned
  type InstanceInfo SecondDb = DatabaseMapping
  instanceInfo (dbSettings, Versioned batch rev, tableName) =
    DatabaseMapping (toS tableName) S3 Parquet (concat ["s3://myprefix/", dbName, "/batch_", batch, "/rev_", show rev, "/"]) schema
      where schema@(dbName, _) = dbSchema dbSettings
secondDb :: DatabaseSettings be SecondDb
secondDb = defaultDbSettings


downsample :: (Sql92SelectSanityCheck syntax, IsSql92SelectSyntax syntax,
               IsSqlExpressionSyntaxStringType (Sql92SelectTableExpressionSyntax (Sql92SelectSelectTableSyntax syntax)) Text,
               HasSqlValueSyntax (Sql92ExpressionValueSyntax (Sql92SelectTableExpressionSyntax (Sql92SelectSelectTableSyntax syntax))) Text,
               HasSqlValueSyntax (Sql92ExpressionValueSyntax (Sql92SelectTableExpressionSyntax (Sql92SelectSelectTableSyntax syntax))) Double
               )
     => Q syntax db s (ModelDataT (QExpr (Sql92SelectExpressionSyntax syntax) s))
     -> Q syntax db s (ModelDataT (QExpr (Sql92SelectExpressionSyntax syntax) s))
downsample tbl =
  do rows <- tbl
     guard_ (model_data_sid rows `like_` (val_ "%215"))
     return rows {model_data_score = model_data_score rows * 0.778899 + 1}


instance IsSqlExpressionSyntaxStringType (Expression DatabaseMapping) Text

-- downAst :: (Select DatabaseMapping, Insert DatabaseMapping)
-- downAst = (ast, astInsert)
--  where
--    versioned = Versioned "WW40" 2
--
--    ast' :: IsSqlExpressionSyntaxStringType (Expression DatabaseMapping) Text => SqlSelect (AST.Select DatabaseMapping) (ModelDataT Identity)
--    ast' = select $ downsample (tAll_ versioned _firstDbCf firstDb)
--    SqlSelect ast = ast'
--    --
--    astInsert' :: SqlInsert (AST.Insert DatabaseMapping)
--    astInsert' = tInsert versioned _secondDbCfDown secondDb $ insertFrom
--                                                        $ select
--                                                        $ downsample (tAll_ versioned _firstDbCf firstDb)
--    SqlInsert astInsert = astInsert'

command :: ProcessingStep DatabaseMapping
command = ProcessingStep "DownsizingModel" etl
  where
    etl :: Versioned -> Insert DatabaseMapping
    etl versioned = astInsert
      where
        astInsert' :: SqlInsert (AST.Insert DatabaseMapping)
        astInsert' = tInsert versioned _secondDbModel secondDb $ insertFrom
                                                            $ select
                                                            $ downsample (tAll_ versioned _firstDbModel firstDb)
        SqlInsert astInsert = astInsert'



entry :: IO ()
entry = do
  print $ computeDAG [command] (Versioned "WW40" 2)
  executeSinglePhrase $ SQLStatement 0 False (InsertCommand $ etl (Versioned "WW40" 2))
  where ProcessingStep _ etl = command
