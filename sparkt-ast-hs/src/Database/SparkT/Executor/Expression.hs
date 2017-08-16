{-# LANGUAGE FlexibleInstances, FlexibleContexts, AllowAmbiguousTypes,
             OverloadedStrings, ScopedTypeVariables #-}
module Database.SparkT.Executor.Expression where

import Control.Monad
import Control.Monad.Except

import Data.Typeable
import Data.String.Conv
import Data.Map as M
import Data.List as L
import Data.Either

import Database.SparkT.AST.Database
import Database.SparkT.AST.SQL
import Database.SparkT.AST.Error
import Database.SparkT.Executor.Context

-- Work in progress

forceAsInt (Value v) =
  case cast v of Just (x :: Integer) -> x
                 _ -> error "dynamic cast failed"

forceAsDouble (Value v) =
 case cast v of Just (x :: Double) -> x
                _ -> error "dynamic cast failed"

forceAsBool (Value v) =
 case cast v of Just (x :: Bool) -> x
                _ -> error "dynamic cast failed"

getAsInt (Value v) =
  case cast v of Just (i :: Integer) -> return i
                 Nothing -> throwError $ ExpressionTypeMismatchError "Integer" ""
getAsDouble (Value v) =
 case cast v of Just (d :: Double) -> return d
                Nothing -> throwError $ ExpressionTypeMismatchError "Double" ""

getAsBool (Value v) =
 case cast v of Just (b :: Bool) -> return b
                Nothing -> throwError $ ExpressionTypeMismatchError "Bool" ""

unify frame lhs rhs = do
  rLhs <- evalE frame lhs
  rRhs <- evalE frame rhs
  if rType rLhs == rType rRhs
    then return (rType rLhs, rLhs, rRhs)
    else throwError $ ExpressionTypeMismatchError (show $ rType rLhs) (show $ rType rRhs)

zipCol f (Col _ s0 t0 n0 r0)
         (Col _ s1 t1 n1 r1) = return $ Col "?" "" t0 n0 (liftM2 (zipWith f) r0 r1)

evalE frame (ExpressionBinOp op eLhs eRhs) = do
  (t, lhs, rhs) <- unify frame eLhs eRhs
  case (t, op) of
    (EInt, "+") -> zipCol (\v w -> Value $ (forceAsInt v) + (forceAsInt w)) lhs rhs
    (EInt, "-") -> zipCol (\v w -> Value $ (forceAsInt v) - (forceAsInt w)) lhs rhs
    (EInt, "*") -> zipCol (\v w -> Value $ (forceAsInt v) * (forceAsInt w)) lhs rhs
    (EDouble, "+") -> zipCol (\v w -> Value $ (forceAsDouble v) + (forceAsDouble w)) lhs rhs
    (EDouble, "-") -> zipCol (\v w -> Value $ (forceAsDouble v) - (forceAsDouble w)) lhs rhs
    (EDouble, "*") -> zipCol (\v w -> Value $ (forceAsDouble v) * (forceAsDouble w)) lhs rhs
    (EDouble, "/") -> zipCol (\v w -> Value $ (forceAsDouble v) / (forceAsDouble w)) lhs rhs
    (EBool, "AND") -> zipCol (\v w -> Value $ (forceAsBool v) && (forceAsBool w)) lhs rhs
    (EBool, "OR") -> zipCol (\v w -> Value $ (forceAsBool v) || (forceAsBool w)) lhs rhs

evalE frame (ExpressionCompOp op _ eLhs eRhs) = do
  (t, lhs, rhs) <- unify frame eLhs eRhs
  case op of
    "=" -> return $ Col "?" "" EBool (rNullable lhs || rNullable rhs) (liftM2 (zipWith (\v w -> Value (v == w))) (rRepr lhs) (rRepr rhs))

evalE _ (ExpressionValue v) = do
  i <- (getAsInt v >> return True) `catchError` (\_ -> return False)
  if i then return $ Col "" "" EInt False (return $ L.repeat v)
       else throwError $ ExecutorUnknownTypeError "" ""

evalE frame (ExpressionFieldName (QualifiedField scope name)) =
  getColumn frame (toS name) (Just $ toS scope)
evalE frame (ExpressionFieldName (UnqualifiedField name)) =
  getColumn frame (toS name) Nothing



evalE _ expr = throwError $ ExecutorNotImplementedError "evalE" (show expr)
