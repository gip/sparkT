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
  if fst (rType rLhs) == fst (rType rRhs)
    then return (fst (rType rLhs), rLhs, rRhs)
    else throwError $ ExpressionTypeMismatchError (show $ rType rLhs) (show $ rType rRhs)

zipCol e f (Col _ s0 (t0, _) n0 _ r0)
           (Col _ s1 (t1, _) n1 _ r1) = return $ Col "?" "" (t0, e) (n0 || n1) Nothing (liftM2 (zipWith f) r0 r1)

evalE frame e@(ExpressionBinOp op eLhs eRhs) = do
  (t, lhs, rhs) <- unify frame eLhs eRhs
  case (t, op) of
    (EInt, "+") -> zipCol e (\v w -> Value $ (forceAsInt v) + (forceAsInt w)) lhs rhs
    (EInt, "-") -> zipCol e (\v w -> Value $ (forceAsInt v) - (forceAsInt w)) lhs rhs
    (EInt, "*") -> zipCol e (\v w -> Value $ (forceAsInt v) * (forceAsInt w)) lhs rhs
    (EDouble, "+") -> zipCol e (\v w -> Value $ (forceAsDouble v) + (forceAsDouble w)) lhs rhs
    (EDouble, "-") -> zipCol e (\v w -> Value $ (forceAsDouble v) - (forceAsDouble w)) lhs rhs
    (EDouble, "*") -> zipCol e (\v w -> Value $ (forceAsDouble v) * (forceAsDouble w)) lhs rhs
    (EDouble, "/") -> zipCol e (\v w -> Value $ (forceAsDouble v) / (forceAsDouble w)) lhs rhs
    (EBool, "AND") -> zipCol e (\v w -> Value $ (forceAsBool v) && (forceAsBool w)) lhs rhs
    (EBool, "OR") -> zipCol e (\v w -> Value $ (forceAsBool v) || (forceAsBool w)) lhs rhs

evalE frame e@(ExpressionCompOp op _ eLhs eRhs) = do
  (t, lhs, rhs) <- unify frame eLhs eRhs
  case op of
    "=" -> doComp e (==) lhs rhs
    "<=" -> doComp e (<=) lhs rhs
    "<" -> doComp e (<) lhs rhs
    ">=" -> doComp e (>=) lhs rhs
    ">" -> doComp e (>) lhs rhs
    "<>" -> doComp e (/=) lhs rhs
  where
    doComp e f lhs rhs =
      return $ Col "?" "" (EBool, e)
                          (rNullable lhs || rNullable rhs)
                          Nothing
                          (liftM2 (zipWith (\v w -> Value (f v w))) (rRepr lhs) (rRepr rhs))

evalE _ e@(ExpressionValue v) = do
  i <- (getAsInt v >> return True) `catchError` (\_ -> return False)
  if i then return $ Col "" "" (EInt, e) False (Just v) (return $ L.repeat v)
       else throwError $ ExecutorUnknownTypeError "" ""

evalE frame e@(ExpressionFieldName (QualifiedField scope name)) =
  col >>= \c -> return c { rType = (rType c, e) }
  where
    col = getColumn frame (toS name) (Just $ toS scope)
evalE frame e@(ExpressionFieldName (UnqualifiedField name)) =
  col >>= \c -> return c { rType = (rType c, e) }
  where
    col = getColumn frame (toS name) Nothing

evalE _ expr = throwError $ ExecutorNotImplementedError "evalE" (show expr)
