{-# LANGUAGE DeriveGeneric, StandaloneDeriving, TypeSynonymInstances,
             FlexibleInstances, TypeFamilies, FlexibleContexts,
             ScopedTypeVariables, PartialTypeSignatures,
             MultiParamTypeClasses, RankNTypes, AllowAmbiguousTypes,
             OverloadedStrings #-}
module Example.ETLs where

import Database.Beam
import Database.Beam.Backend.SQL
import Database.SparkT.AST as AST

import Database.SparkT.AST.Protocol
import Database.SparkT.AST.Database
import Database.SparkT.AST.ETL

import Database.Beam.Backend.SQL.Builder

import Data.Text (Text)

import Example.Databases

downsample :: (Sql92SelectSanityCheck syntax, IsSql92SelectSyntax syntax,
               IsSqlExpressionSyntaxStringType (Sql92SelectTableExpressionSyntax (Sql92SelectSelectTableSyntax syntax)) Text,
               HasSqlValueSyntax (Sql92ExpressionValueSyntax (Sql92SelectTableExpressionSyntax (Sql92SelectSelectTableSyntax syntax))) Text,
               HasSqlValueSyntax (Sql92ExpressionValueSyntax (Sql92SelectTableExpressionSyntax (Sql92SelectSelectTableSyntax syntax))) Double
               )
     => Q syntax db s (ModelAttributeDataT (QExpr (Sql92SelectExpressionSyntax syntax) s))
     -> Q syntax db s (ModelAttributeDataT (QExpr (Sql92SelectExpressionSyntax syntax) s))
downsample tbl =
  do rows <- tbl
     guard_ (model_attr_data_sid rows `like_` (val_ "%215"))
     return rows {model_attr_data_score = model_attr_data_score rows * 0.778899 + 1}


instance IsSqlExpressionSyntaxStringType (Expression DatabaseMapping) Text

command :: Versioned -> ProcessingStep DatabaseMapping
command versioned = ProcessingStep "DownsizingModel" etl
  where
    etl :: Insert DatabaseMapping
    etl = astInsert
      where
        astInsert' :: SqlInsert (AST.Insert DatabaseMapping)
        astInsert' = tInsert versioned _secondDbModelAttr secondDb
                       $ insertFrom
                         $ select
                           $ downsample
                             (do
                                model <- tAll_ versioned _firstDbModel firstDb
                                attr <- tAll_ versioned _firstDbAttribute firstDb
                                guard_ (model_data_sid model ==. attr_data_sid attr)
                                pure $ ModelAttributeData {
                                  model_attr_data_sid = model_data_sid model,
                                  model_attr_data_item = model_data_item model,
                                  model_attr_data_score = model_data_score model,
                                  model_attr_data_name = attr_data_name attr }
                              )
        SqlInsert astInsert = astInsert'
