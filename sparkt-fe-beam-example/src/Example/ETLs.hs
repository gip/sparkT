{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, TypeFamilies,
    FlexibleContexts, PartialTypeSignatures, MultiParamTypeClasses,
    RankNTypes, AllowAmbiguousTypes, OverloadedStrings #-}
module Example.ETLs where

import Data.Text (Text)
import Data.Typeable

import Database.Beam
import Database.Beam.Backend.SQL
import Database.Beam.Backend.SQL.Builder

import Database.SparkT.AST as AST
import Database.SparkT.AST.Protocol
import Database.SparkT.AST.Database
import Database.SparkT.AST.ETL

import Example.Schemata

downsample :: (Sql92SelectSanityCheck syntax, IsSql92SelectSyntax syntax,
               IsSqlExpressionSyntaxStringType (Sql92SelectTableExpressionSyntax (Sql92SelectSelectTableSyntax syntax)) Text,
               HasSqlValueSyntax (Sql92ExpressionValueSyntax (Sql92SelectTableExpressionSyntax (Sql92SelectSelectTableSyntax syntax))) Text,
               HasSqlValueSyntax (Sql92ExpressionValueSyntax (Sql92SelectTableExpressionSyntax (Sql92SelectSelectTableSyntax syntax))) Double
               )
     => Q syntax db s (ModelAttributeDataT (QExpr (Sql92SelectExpressionSyntax syntax) s))
     -> Q syntax db s (ModelAttributeDataT (QExpr (Sql92SelectExpressionSyntax syntax) s))
downsample tbl =
  do rows <- tbl
     guard_ (ma_sid rows `like_` val_ "%214")
     return rows {ma_score = ma_score rows * 0.778899 + 1}


noSampling :: Q syntax db s (ModelAttributeDataT (QExpr (Sql92SelectExpressionSyntax syntax) s))
          -> Q syntax db s (ModelAttributeDataT (QExpr (Sql92SelectExpressionSyntax syntax) s))
noSampling tbl = tbl

instance IsSqlExpressionSyntaxStringType (Expression (DatabaseMapping TypeRep)) Text

joinStep :: Versioned -> Step (DatabaseMapping TypeRep)
joinStep versioned = Step "joinStep" etl
  where
    SqlInsert etl = tInsert versioned acmeDbModelAttr acmeDb
                      $ insertFrom
                        $ select
                          $ noSampling
                            $ do model <- tAll_ versioned acmeDbModel acmeDb
                                 attr <- tAll_ versioned acmeDbAttr acmeDb
                                 guard_ (m_sid model ==. a_sid attr)                 -- Note the JOIN!
                                 pure ModelAttributeData {
                                   ma_sid = m_sid model,
                                   ma_item = m_item model,
                                   ma_score = m_score model,
                                   ma_name = a_name attr }

downsampleStep :: Versioned -> Step (DatabaseMapping TypeRep)
downsampleStep versioned = Step "downsampleStep" etl
  where
    SqlInsert etl = tInsert versioned apexDbModelAttr apexDb
                   $ insertFrom
                     $ select
                       $ downsample
                         $ tAll_ versioned acmeDbModelAttr acmeDb

acmeETL versioned = buildETL "acmeETL" [downsampleStep versioned,
                                        joinStep versioned]

-- Let's add some test data ----------------------------------------------------
loadAcmeModel :: Versioned -> Step (DatabaseMapping TypeRep)
loadAcmeModel versioned = Step "loadAcmeModel" etl
  where
    SqlInsert etl = tInsert versioned acmeDbModel acmeDb
                     $ insertValues [ModelData "78ef30ab" 45 6.7,
                                     ModelData "abcd0215" 46 0.0,
                                     ModelData "efd56214" 47 0.3,
                                     ModelData "0ef34800" 48 5.6]

loadAcmeAttr :: Versioned -> Step (DatabaseMapping TypeRep)
loadAcmeAttr versioned = Step "loadAcmeModel" etl
 where
   SqlInsert etl = tInsert versioned acmeDbAttr acmeDb
                    $ insertValues [AttributeData "abcd0215" "Dmitri",
                                    AttributeData "efd56214" "Alyosha",
                                    AttributeData "0ef34800" "Ivan",
                                    AttributeData "aef34215" "Yefim",
                                    AttributeData "78ef30ab" "Fyodor"]

acmeDataLoad versioned = buildETL "acmeDataLoad"
                              [loadAcmeModel versioned,
                               loadAcmeAttr versioned]

acmeETLWithData versioned = buildETL "acmeETLWithData"
                              [downsampleStep versioned,
                               joinStep versioned,
                               loadAcmeModel versioned,
                               loadAcmeAttr versioned]
