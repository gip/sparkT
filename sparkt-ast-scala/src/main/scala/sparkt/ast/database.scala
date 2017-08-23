package sparkt.ast.database

abstract class ATType
case class TInt() extends ATType
case class TString() extends ATType
case class TText() extends ATType
case class TDouble() extends ATType
case class TBool() extends ATType

abstract class ADStorage
case class DS3() extends ADStorage
case class DPostgresSQL() extends ADStorage
case class DRedshift() extends ADStorage
case class DCache() extends ADStorage

abstract class ADFormat
case class DParquet() extends ADFormat
case class DCSV(delim: String) extends ADFormat
case class DAutodetect() extends ADFormat
case class DProprietary() extends ADFormat

abstract class ADDatabaseMapping
case class DDatabaseMapping(table: String,
                            storage: ADStorage,
                            fmt: ADFormat,
                            url: String,
                            schema: (String, Seq[(String, Seq[(String, ATType, Boolean)])])) extends ADDatabaseMapping
