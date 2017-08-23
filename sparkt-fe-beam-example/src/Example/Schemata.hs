{-# LANGUAGE DeriveGeneric, StandaloneDeriving, TypeSynonymInstances,
             FlexibleInstances, TypeFamilies, FlexibleContexts,
             ScopedTypeVariables, PartialTypeSignatures,
             MultiParamTypeClasses, RankNTypes, AllowAmbiguousTypes,
             OverloadedStrings #-}
module Example.Schemata where

import Data.Text (Text)
import Data.String.Conv
import Data.Typeable

import Database.Beam
import Database.Beam.Backend.SQL
import Database.SparkT.AST as AST

import Database.SparkT.AST.Database

-- A table --------------------------------------------------------------------
data ModelDataT f
  = ModelData
  { m_sid   :: Columnar f Text
  , m_item  :: Columnar f Int
  , m_score :: Columnar f Double
  } deriving Generic
type ModelData = ModelDataT Identity
type ModelDataId = PrimaryKey ModelDataT Identity
deriving instance Show ModelData
deriving instance Eq ModelData
instance Beamable ModelDataT
instance Table ModelDataT where
  data PrimaryKey ModelDataT f = ModelDataId (Columnar f Text) deriving Generic
  primaryKey = ModelDataId . m_sid
instance Beamable (PrimaryKey ModelDataT)

-- A table --------------------------------------------------------------------
data AttributeDataT f
  = AttributeData
  { a_sid   :: Columnar f Text,
    a_name  :: Columnar f Text
  } deriving Generic
type AttributeData = AttributeDataT Identity
type AttributeDataId = PrimaryKey AttributeDataT Identity
deriving instance Show AttributeData
deriving instance Eq AttributeData
instance Beamable AttributeDataT
instance Table AttributeDataT where
  data PrimaryKey AttributeDataT f = AttributeDataId (Columnar f Text) deriving Generic
  primaryKey = AttributeDataId . a_sid
instance Beamable (PrimaryKey AttributeDataT)

-- A table --------------------------------------------------------------------
data ModelAttributeDataT f
  = ModelAttributeData
  { ma_sid    :: Columnar f Text,
    ma_item   :: Columnar f Int,
    ma_score  :: Columnar f Double,
    ma_name   :: Columnar f Text
  } deriving Generic
type ModelAttributeData = ModelAttributeDataT Identity
type ModelAttributeDataId = PrimaryKey ModelAttributeDataT Identity
deriving instance Show ModelAttributeData
deriving instance Eq ModelAttributeData
instance Beamable ModelAttributeDataT
instance Table ModelAttributeDataT where
  data PrimaryKey ModelAttributeDataT f = ModelAttributeDataId (Columnar f Text) deriving Generic
  primaryKey = ModelAttributeDataId . ma_sid
instance Beamable (PrimaryKey ModelAttributeDataT)

-- A database -----------------------------------------------------------------
data AcmeDb f = AcmeDb {
  acmeDbModel     :: f (TableEntity ModelDataT),
  acmeDbAttr      :: f (TableEntity AttributeDataT),
  acmeDbModelAttr :: f (TableEntity ModelAttributeDataT)
} deriving Generic
instance Database AcmeDb where
  type InstanceVersioned AcmeDb = Versioned
  type InstanceInfo AcmeDb = (DatabaseMapping TypeRep)
  instanceInfo (dbSettings, Versioned batch rev, tableName) =
    DatabaseMapping (toS tableName) S3 (CSV "|") (concat ["s3://myprefix/", dbName, "/batch_", batch, "/rev_", show rev, "/"]) schema
      where schema@(dbName, _) = dbSchema dbSettings
acmeDb :: DatabaseSettings be AcmeDb
acmeDb = defaultDbSettings

-- A database -----------------------------------------------------------------
data ApexDb f = ApexDb {
  apexDbModel     :: f (TableEntity ModelDataT),
  apexDbModelAttr :: f (TableEntity ModelAttributeDataT)
} deriving Generic
instance Database ApexDb where
  type InstanceVersioned ApexDb = Versioned
  type InstanceInfo ApexDb = (DatabaseMapping TypeRep)
  instanceInfo (dbSettings, Versioned batch rev, tableName) =
    DatabaseMapping (toS tableName) S3 Parquet (concat ["s3://myprefix/", dbName, "/batch_", batch, "/rev_", show rev, "/"]) schema
      where schema@(dbName, _) = dbSchema dbSettings
apexDb :: DatabaseSettings be ApexDb
apexDb = defaultDbSettings
