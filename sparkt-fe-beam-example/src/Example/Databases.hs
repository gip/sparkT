{-# LANGUAGE DeriveGeneric, StandaloneDeriving, TypeSynonymInstances,
             FlexibleInstances, TypeFamilies, FlexibleContexts,
             ScopedTypeVariables, PartialTypeSignatures,
             MultiParamTypeClasses, RankNTypes, AllowAmbiguousTypes,
             OverloadedStrings #-}
module Example.Databases where

import Data.Text (Text)
import Data.String.Conv

import Database.Beam
import Database.Beam.Backend.SQL
import Database.SparkT.AST as AST

import Database.SparkT.AST.Database

-- A table --------------------------------------------------------------------
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

-- A table --------------------------------------------------------------------
data AttributeDataT f
  = AttributeData
  { attr_data_sid   :: Columnar f Text,
    attr_data_name  :: Columnar f Text
  } deriving Generic
type AttributeData = AttributeDataT Identity
type AttributeDataId = PrimaryKey AttributeDataT Identity
deriving instance Show AttributeData
deriving instance Eq AttributeData
instance Beamable AttributeDataT
instance Table AttributeDataT where
  data PrimaryKey AttributeDataT f = AttributeDataId (Columnar f Text) deriving Generic
  primaryKey = AttributeDataId . attr_data_sid
instance Beamable (PrimaryKey AttributeDataT)

-- A table --------------------------------------------------------------------
data ModelAttributeDataT f
  = ModelAttributeData
  { model_attr_data_sid    :: Columnar f Text,
    model_attr_data_item   :: Columnar f Int,
    model_attr_data_score  :: Columnar f Double,
    model_attr_data_name   :: Columnar f Text
  } deriving Generic
type ModelAttributeData = ModelAttributeDataT Identity
type ModelAttributeDataId = PrimaryKey ModelAttributeDataT Identity
deriving instance Show ModelAttributeData
deriving instance Eq ModelAttributeData
instance Beamable ModelAttributeDataT
instance Table ModelAttributeDataT where
  data PrimaryKey ModelAttributeDataT f = ModelAttributeDataId (Columnar f Text) deriving Generic
  primaryKey = ModelAttributeDataId . model_attr_data_sid
instance Beamable (PrimaryKey ModelAttributeDataT)

-- A database -----------------------------------------------------------------
data FirstDb f = FirstDb {
  _firstDbModel     :: f (TableEntity ModelDataT),
  _firstDbAttribute :: f (TableEntity AttributeDataT)
} deriving Generic
instance Database FirstDb where
  type InstanceVersioned FirstDb = Versioned
  type InstanceInfo FirstDb = DatabaseMapping
  instanceInfo (dbSettings, Versioned batch rev, tableName) =
    DatabaseMapping (toS tableName) S3 (CSV "|") (concat ["s3://myprefix/", dbName, "/batch_", batch, "/rev_", show rev, "/"]) schema
      where schema@(dbName, _) = dbSchema dbSettings
firstDb :: DatabaseSettings be FirstDb
firstDb = defaultDbSettings

-- A database -----------------------------------------------------------------
data SecondDb f = SecondDb {
  _secondDbModel     :: f (TableEntity ModelDataT),
  _secondDbModelAttr :: f (TableEntity ModelAttributeDataT)
} deriving Generic
instance Database SecondDb where
  type InstanceVersioned SecondDb = Versioned
  type InstanceInfo SecondDb = DatabaseMapping
  instanceInfo (dbSettings, Versioned batch rev, tableName) =
    DatabaseMapping (toS tableName) S3 Parquet (concat ["s3://myprefix/", dbName, "/batch_", batch, "/rev_", show rev, "/"]) schema
      where schema@(dbName, _) = dbSchema dbSettings
secondDb :: DatabaseSettings be SecondDb
secondDb = defaultDbSettings
