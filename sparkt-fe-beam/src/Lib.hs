{-# LANGUAGE DeriveGeneric, StandaloneDeriving, TypeSynonymInstances,
             FlexibleInstances, TypeFamilies, FlexibleContexts,
             ScopedTypeVariables, PartialTypeSignatures,
             MultiParamTypeClasses, RankNTypes, AllowAmbiguousTypes,
             OverloadedStrings #-}
module Lib where

import Database.Beam
import Database.Beam.Backend.SQL
import Database.SparkT.AST as AST
import Database.Beam.Backend.SQL.Builder

import Data.Text (Text)
import Data.String.Conv
import Data.Aeson
import qualified Data.ByteString.Lazy.Char8 as BSL (putStrLn)


import Data.Proxy
import Data.Data
import GHC.Generics


-- A table
data CfModelDataT f
  = CfModelData
  { model_data_sid   :: Columnar f Text
  , model_data_item  :: Columnar f Int
  , model_data_score :: Columnar f Double
  , weird_field      :: Columnar f Int
  } deriving Generic
type CfModelData = CfModelDataT Identity
type CfModelDataId = PrimaryKey CfModelDataT Identity
deriving instance Show CfModelData
deriving instance Eq CfModelData
instance Beamable CfModelDataT
instance Table CfModelDataT where
  data PrimaryKey CfModelDataT f = CfModelDataId (Columnar f Text) deriving Generic
  primaryKey = CfModelDataId . model_data_sid
instance Beamable (PrimaryKey CfModelDataT)

data Storage = S3 | PostgresSQL | Redshift
  deriving (Eq, Show, Generic)
data Format = Parquet | CSV String | NA
  deriving (Eq, Show, Generic)
instance ToJSON Format

instance ToJSON Storage
data Versioned = Versioned {
  batchId :: String,
  revisionId :: Int
} deriving (Eq, Show, Generic)

data DatabaseMapping = DatabaseMapping {
  storage :: Storage,
  format :: Format,
  url :: String,
  schema :: DatabaseSchema
} deriving (Eq, Show, Generic)
instance ToJSON DatabaseMapping


data FirstDb f = FirstDb {
  _firstDbCf     :: f (TableEntity CfModelDataT),
  _firstDbCfDown :: f (TableEntity CfModelDataT)
} deriving Generic
instance Database FirstDb where
  type InstanceVersioned FirstDb = Versioned
  type InstanceInfo FirstDb = DatabaseMapping
  instanceInfo (dbSettings, Versioned batch rev) =
    DatabaseMapping S3 (CSV "|") (concat ["s3://myprefix/", dbName, "/batch_", batch, "/rev_", show rev, "/"]) schema
      where schema@(dbName, _) = dbSchema dbSettings
firstDb :: DatabaseSettings be FirstDb
firstDb = defaultDbSettings

data SecondDb f = SecondDb {
  _secondDbCf     :: f (TableEntity CfModelDataT),
  _secondDbCfDown :: f (TableEntity CfModelDataT)
} deriving Generic
instance Database SecondDb where
  type InstanceVersioned SecondDb = Versioned
  type InstanceInfo SecondDb = DatabaseMapping
  instanceInfo (dbSettings, Versioned batch rev) =
    DatabaseMapping Redshift NA (concat ["redshift:://myprefix/", dbName, "/batch_", batch, "/rev_", show rev, "/"]) schema
      where schema@(dbName, _) = dbSchema dbSettings
secondDb :: DatabaseSettings be SecondDb
secondDb = defaultDbSettings



downsample :: (Sql92SelectSanityCheck syntax, IsSql92SelectSyntax syntax,
               IsSqlExpressionSyntaxStringType (Sql92SelectTableExpressionSyntax (Sql92SelectSelectTableSyntax syntax)) Text,
               HasSqlValueSyntax (Sql92ExpressionValueSyntax (Sql92SelectTableExpressionSyntax (Sql92SelectSelectTableSyntax syntax))) Text,
               HasSqlValueSyntax (Sql92ExpressionValueSyntax (Sql92SelectTableExpressionSyntax (Sql92SelectSelectTableSyntax syntax))) Double
               )
     => Q syntax db s (CfModelDataT (QExpr (Sql92SelectExpressionSyntax syntax) s))
     -> Q syntax db s (CfModelDataT (QExpr (Sql92SelectExpressionSyntax syntax) s))
downsample tbl =
  do rows <- tbl
     guard_ (model_data_sid rows `like_` (val_ "%215"))
     return rows {model_data_score = model_data_score rows * 0.778899 + 1}


instance IsSqlExpressionSyntaxStringType (Expression DatabaseMapping) Text
downAst :: (Select DatabaseMapping, Insert DatabaseMapping)
downAst = (ast, astInsert)
 where
   versioned = Versioned "WW40" 2

   ast' :: IsSqlExpressionSyntaxStringType (Expression DatabaseMapping) Text => SqlSelect (AST.Select DatabaseMapping) (CfModelDataT Identity)
   ast' = select $ downsample (tAll_ versioned _firstDbCf firstDb)
   SqlSelect ast = ast'
   --
   astInsert' :: SqlInsert (AST.Insert DatabaseMapping)
   astInsert' = tInsert versioned _secondDbCfDown secondDb $ insertFrom
                                                       $ select
                                                       $ downsample (tAll_ versioned _firstDbCf firstDb)
   SqlInsert astInsert = astInsert'


someFunc :: IO ()
someFunc = BSL.putStrLn (encode a) >> BSL.putStrLn (encode b)
  where (a,b) = downAst
