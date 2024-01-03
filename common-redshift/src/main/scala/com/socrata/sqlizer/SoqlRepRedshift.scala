package com.socrata.common.sqlizer

import java.sql.ResultSet

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.json.v3.util.JsonUtil
import com.vividsolutions.jts.geom.{Geometry}

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance
import com.socrata.soql.types._
import com.socrata.soql.sqlizer._

/*
 Ensure compressedSubColumns works in all cases
 Use the extractor in soqlreference
 */

abstract class SoQLRepProviderRedshift[MT <: MetaTypes with metatypes.SoQLMetaTypesExt with ({
  type ColumnType = SoQLType; type ColumnValue = SoQLValue; type DatabaseColumnNameImpl = String
})](
    cryptProviders: CryptProviderProvider,
    override val namespace: SqlNamespaces[MT],
    override val exprSqlFactory: ExprSqlFactory[MT]
) extends Rep.Provider[MT] {
  // TODO: obvious not good
  override val isRollup = _ => ???
  override val toProvenance = _ => ???

  def apply(typ: SoQLType) = reps(typ)

  override def mkTextLiteral(s: String): Doc =
    d"text" +#+ mkStringLiteral(s)
  override def mkByteaLiteral(bytes: Array[Byte]): Doc =
    mkStringLiteral(bytes.iterator.map { b => "%02x".format(b & 0xff) }.mkString)

  abstract class GeometryRep[T <: Geometry](t: SoQLType with SoQLGeometryLike[T], ctor: T => CV)
      extends SingleColumnRep(t, d"geometry") {
    override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) = ???
    private val open = d"ST_GeomFromWKB"

    override def literal(e: LiteralValue) = {
      val geo = downcast(e.value)
      exprSqlFactory(Seq(mkByteaLiteral(t.WkbRep(geo)), Geo.defaultSRIDLiteral).funcall(open), e)
    }

    protected def downcast(v: SoQLValue): T

    override def hasTopLevelWrapper = true
    override def wrapTopLevel(raw: ExprSql) = {
      assert(raw.typ == typ)
      exprSqlFactory(raw.compressed.sql.funcall(d"st_asbinary"), raw.expr)
    }

    override def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
      Option(rs.getBytes(dbCol)).flatMap { bytes =>
        t.WkbRep.unapply(
          bytes
        ) // TODO: this just turns invalid values into null, we should probably be noisier than that
      }.map(ctor).getOrElse(SoQLNull)
    }
  }

  val reps = Map[SoQLType, Rep](
    SoQLID -> new ProvenancedRep(SoQLID, d"bigint") {
      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) = ???

      override def compressedDatabaseType = d"jsonb"

      override def provenanceOf(e: LiteralValue) = { // test this
        val rawId = e.value.asInstanceOf[SoQLID]
        Set(rawId.provenance)
      }

      override def compressedSubColumns(table: String, column: ColumnLabel) = {
        val sourceName = compressedDatabaseColumn(column)
        val Seq(provenancedName, dataName) = expandedDatabaseColumns(column)
        Seq(
          // this'll need to be using our special compression thing
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 0 AS" +#+ provenancedName,
          d"((" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 1) :: bigint AS" +#+ dataName
        )
      }

      override def literal(e: LiteralValue) = {
        val rawId = e.value.asInstanceOf[SoQLID]
        val rawFormatted = SoQLID.FormattedButUnobfuscatedStringRep(rawId)
        // ok, "rawFormatted" is the string as the user entered it.
        // Now we want to examine with the appropriate
        // CryptProvider...

        val provenanceLit =
          rawId.provenance match {
            case None => d"null :: text"
            case Some(Provenance(s)) => mkTextLiteral(s)
          }
        val numLit =
          rawId.provenance.flatMap(cryptProviders.forProvenance) match {
            case None =>
              Doc(rawId.value.toString) +#+ d":: bigint"
            case Some(cryptProvider) =>
              val idStringRep = new SoQLID.StringRep(cryptProvider)
              val SoQLID(num) = idStringRep.unapply(rawFormatted).get
              Doc(num.toString) +#+ d":: bigint"
          }

        exprSqlFactory(Seq(provenanceLit, numLit), e)
      }

      override protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        val provenance = Option(rs.getString(dbCol)).map(Provenance(_))
        val valueRaw = rs.getLong(dbCol + 1)

        if (rs.wasNull) {
          SoQLNull
        } else {
          val result = SoQLID(valueRaw)
          result.provenance = provenance
          result
        }
      }

      override protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getString(dbCol)) match {
          case None =>
            SoQLNull
          case Some(v) =>
            JsonUtil.parseJson[(Either[JNull, String], Long)](v) match {
              case Right((Right(prov), v)) =>
                val result = SoQLID(v)
                result.provenance = Some(Provenance(prov))
                result
              case Right((Left(JNull), v)) =>
                SoQLID(v)
              case Left(err) =>
                throw new Exception(err.english)
            }
        }
      }
    },
    SoQLVersion -> new ProvenancedRep(SoQLVersion, d"bigint") {
      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) = ???

      override def compressedDatabaseType = d"jsonb"

      override def provenanceOf(e: LiteralValue) = {
        val rawId = e.value.asInstanceOf[SoQLVersion]
        Set(rawId.provenance)
      }

      override def compressedSubColumns(table: String, column: ColumnLabel) = {
        val sourceName = compressedDatabaseColumn(column)
        val Seq(provenancedName, dataName) = expandedDatabaseColumns(column)
        Seq(
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 0 AS" +#+ provenancedName,
          d"((" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 1) :: bigint AS" +#+ dataName
        )
      }

      override def literal(e: LiteralValue) = {
        val rawId = e.value.asInstanceOf[SoQLVersion]
        val rawFormatted = SoQLVersion.FormattedButUnobfuscatedStringRep(rawId)
        // ok, "rawFormatted" is the string as the user entered it.
        // Now we want to examine with the appropriate
        // CryptProvider...

        val provenanceLit =
          rawId.provenance match {
            case None => d"null :: text"
            case Some(Provenance(s)) => mkTextLiteral(s)
          }
        val numLit =
          rawId.provenance.flatMap(cryptProviders.forProvenance) match {
            case None =>
              Doc(rawId.value.toString) +#+ d":: bigint"
            case Some(cryptProvider) =>
              val idStringRep = new SoQLVersion.StringRep(cryptProvider)
              val SoQLVersion(num) = idStringRep.unapply(rawFormatted).get
              Doc(num.toString) +#+ d":: bigint"
          }

        exprSqlFactory(Seq(provenanceLit, numLit), e)
      }

      override protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        val provenance = Option(rs.getString(dbCol)).map(Provenance(_))
        val valueRaw = rs.getLong(dbCol + 1)

        if (rs.wasNull) {
          SoQLNull
        } else {
          val result = SoQLVersion(valueRaw)
          result.provenance = provenance
          result
        }
      }

      override protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getString(dbCol)) match {
          case None =>
            SoQLNull
          case Some(v) =>
            JsonUtil.parseJson[(Either[JNull, String], Long)](v) match {
              case Right((Right(prov), v)) =>
                val result = SoQLVersion(v)
                result.provenance = Some(Provenance(prov))
                result
              case Right((Left(JNull), v)) =>
                SoQLVersion(v)
              case Left(err) =>
                throw new Exception(err.english)
            }
        }
      }
    },

    // ATOMIC REPS

    SoQLText -> new SingleColumnRep(SoQLText, d"text") {
      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) = ???
      override def literal(e: LiteralValue) = {
        val SoQLText(s) = e.value
        exprSqlFactory(mkTextLiteral(s), e)
      }
      override protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getString(dbCol)) match {
          case None => SoQLNull
          case Some(t) => SoQLText(t)
        }
      }
    },
    SoQLNumber -> new SingleColumnRep(SoQLNumber, Doc(SoQLFunctionSqlizerRedshift.numericType)) {
      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) = ???
      override def literal(e: LiteralValue) = {
        val SoQLNumber(n) = e.value
        exprSqlFactory(Doc(n.toString) +#+ d"::" +#+ sqlType, e)
      }
      override protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getBigDecimal(dbCol)) match {
          case None => SoQLNull
          case Some(t) => SoQLNumber(t)
        }
      }
    },
    SoQLBoolean -> new SingleColumnRep(SoQLBoolean, d"boolean") {
      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) = ???
      def literal(e: LiteralValue) = {
        val SoQLBoolean(b) = e.value
        exprSqlFactory(if (b) d"true" else d"false", e)
      }
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        val v = rs.getBoolean(dbCol)
        if (rs.wasNull) {
          SoQLNull
        } else {
          SoQLBoolean(v)
        }
      }
    },
    SoQLFixedTimestamp -> new SingleColumnRep(SoQLFixedTimestamp, d"timestamp with time zone") {
      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) = ???
      def literal(e: LiteralValue) = {
        val SoQLFixedTimestamp(s) = e.value
        exprSqlFactory(sqlType +#+ mkStringLiteral(SoQLFixedTimestamp.StringRep(s)), e)
      }
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        new com.socrata.datacoordinator.common.soql.sqlreps.FixedTimestampRep("").fromResultSet(rs, dbCol)
      }
    },
    SoQLFloatingTimestamp -> new SingleColumnRep(SoQLFloatingTimestamp, d"timestamp without time zone") {
      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) = ???
      def literal(e: LiteralValue) = {
        val SoQLFloatingTimestamp(s) = e.value
        exprSqlFactory(sqlType +#+ mkStringLiteral(SoQLFloatingTimestamp.StringRep(s)), e)
      }
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        new com.socrata.datacoordinator.common.soql.sqlreps.FloatingTimestampRep("").fromResultSet(rs, dbCol)
      }
    },
    SoQLDate -> new SingleColumnRep(SoQLDate, d"date") {
      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) = ???
      def literal(e: LiteralValue) = {
        val SoQLDate(s) = e.value
        exprSqlFactory(sqlType +#+ mkStringLiteral(SoQLDate.StringRep(s)), e)
      }
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        new com.socrata.datacoordinator.common.soql.sqlreps.DateRep("").fromResultSet(rs, dbCol)
      }
    },
    SoQLTime -> new SingleColumnRep(SoQLTime, d"time without time zone") {
      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) = ???
      def literal(e: LiteralValue) = {
        val SoQLTime(s) = e.value
        exprSqlFactory(sqlType +#+ mkStringLiteral(SoQLTime.StringRep(s)), e)
      }
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        new com.socrata.datacoordinator.common.soql.sqlreps.TimeRep("").fromResultSet(rs, dbCol)
      }
    },
    SoQLJson -> new SingleColumnRep(SoQLJson, d"super") { // this'll need to be super
      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) = ???
      def literal(e: LiteralValue) = {
        val SoQLJson(j) = e.value

        val stringRepr = j match {
          case _: JNumber | JNull => Doc(CompactJsonWriter.toString(j))
          case _ => mkStringLiteral(CompactJsonWriter.toString(j))
        }

        exprSqlFactory(stringRepr.funcall(d"JSON_PARSE"), e)
      }
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        new com.socrata.datacoordinator.common.soql.sqlreps.JsonRep("").fromResultSet(rs, dbCol)
      }
    },
    SoQLDocument -> new SingleColumnRep(SoQLDocument, d"super") {
      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) = ???
      override def literal(e: LiteralValue) = ???
      override protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getString(dbCol)) match {
          case Some(s) =>
            JsonUtil.parseJson[SoQLDocument](s) match {
              case Right(doc) => doc
              case Left(err) => throw new Exception("Unexpected document json from database: " + err.english)
            }
          case None =>
            SoQLNull
        }
      }
    },
    SoQLPoint -> new GeometryRep(SoQLPoint, SoQLPoint(_)) {
      override def downcast(v: SoQLValue) = v.asInstanceOf[SoQLPoint].value
    },
    SoQLMultiPoint -> new GeometryRep(SoQLMultiPoint, SoQLMultiPoint(_)) {
      override def downcast(v: SoQLValue) = v.asInstanceOf[SoQLMultiPoint].value
      override def isPotentiallyLarge = true
    },
    SoQLLine -> new GeometryRep(SoQLLine, SoQLLine(_)) {
      override def downcast(v: SoQLValue) = v.asInstanceOf[SoQLLine].value
      override def isPotentiallyLarge = true
    },
    SoQLMultiLine -> new GeometryRep(SoQLMultiLine, SoQLMultiLine(_)) {
      override def downcast(v: SoQLValue) = v.asInstanceOf[SoQLMultiLine].value
      override def isPotentiallyLarge = true
    },
    SoQLPolygon -> new GeometryRep(SoQLPolygon, SoQLPolygon(_)) {
      override def downcast(v: SoQLValue) = v.asInstanceOf[SoQLPolygon].value
      override def isPotentiallyLarge = true
    },
    SoQLMultiPolygon -> new GeometryRep(SoQLMultiPolygon, SoQLMultiPolygon(_)) {
      override def downcast(v: SoQLValue) = v.asInstanceOf[SoQLMultiPolygon].value
      override def isPotentiallyLarge = true
    }
  )
}
