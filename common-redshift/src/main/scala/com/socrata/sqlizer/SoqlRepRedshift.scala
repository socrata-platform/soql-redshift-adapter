package com.socrata.common.sqlizer

import java.sql.ResultSet

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.json.v3.util.OrJNull.implicits._
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.joda.time.Period
import org.postgresql.util.PGInterval

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance
import com.socrata.soql.types._
import com.socrata.soql.sqlizer._


/*
n



Make tests which create a table of every column type



ids, money, url, etc..

 ensure literals can be construced.
 Ensure compressedSubColumns works in all cases
 ensure doExtractFrom works


not sure how to test compression and stuff like that. How do I know

I think I can remove all use of indices https://popsql.com/learn-sql/redshift/how-to-create-an-index-in-redshift


add scalafmt

 */







abstract class SoQLRepProviderRedshift[MT <: MetaTypes with metatypes.SoQLMetaTypesExt with ({type ColumnType = SoQLType; type ColumnValue = SoQLValue; type DatabaseColumnNameImpl = String})](
  cryptProviders: CryptProviderProvider,
  override val exprSqlFactory: ExprSqlFactory[MT],
  override val namespace: SqlNamespaces[MT],
  override val toProvenance: types.ToProvenance[MT],
  override val isRollup: types.DatabaseTableName[MT] => Boolean, // can be removed
  locationSubcolumns: Map[types.DatabaseTableName[MT], Map[types.DatabaseColumnName[MT], Seq[Option[types.DatabaseColumnName[MT]]]]],
  physicalTableFor: Map[AutoTableLabel, types.DatabaseTableName[MT]]
) extends Rep.Provider[MT] {
  def apply(typ: SoQLType) = reps(typ)

  override def mkTextLiteral(s: String): Doc =
    d"text" +#+ mkStringLiteral(s)
  override def mkByteaLiteral(bytes: Array[Byte]): Doc =
    d"bytea" +#+ mkStringLiteral(bytes.iterator.map { b => "%02x".format(b & 0xff) }.mkString("\\x", "", ""))

  abstract class GeometryRep[T <: Geometry](t: SoQLType with SoQLGeometryLike[T], ctor: T => CV, name: String) extends SingleColumnRep(t, d"geometry") {
    private val open = d"st_${name}fromwkb"

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
        t.WkbRep.unapply(bytes) // TODO: this just turns invalid values into null, we should probably be noisier than that
      }.map(ctor).getOrElse(SoQLNull)
    }

    override def indices(tableName: DatabaseTableName, label: ColumnLabel) = Seq.empty
  }

  val reps = Map[SoQLType, Rep](
    SoQLID -> new ProvenancedRep(SoQLID, d"bigint") {
      override def provenanceOf(e: LiteralValue) = { // test this
        val rawId = e.value.asInstanceOf[SoQLID]
        Set(rawId.provenance)
      }

      override def compressedSubColumns(table: String, column: ColumnLabel) = { // TODO: do this
        val sourceName = compressedDatabaseColumn(column)
        val Seq(provenancedName, dataName) = expandedDatabaseColumns(column)
        Seq(
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 0 AS" +#+ provenancedName, // no json
          d"((" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 1) :: bigint AS" +#+ dataName,
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

        if(rs.wasNull) {
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

      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = Seq.empty

    },
    SoQLVersion -> new ProvenancedRep(SoQLVersion, d"bigint") {
      override def provenanceOf(e: LiteralValue) = {
        val rawId = e.value.asInstanceOf[SoQLVersion]
        Set(rawId.provenance)
      }

      override def compressedSubColumns(table: String, column: ColumnLabel) = {
        val sourceName = compressedDatabaseColumn(column)
        val Seq(provenancedName, dataName) = expandedDatabaseColumns(column)
        Seq(
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 0 AS" +#+ provenancedName,
          d"((" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 1) :: bigint AS" +#+ dataName,
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

        if(rs.wasNull) {
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

      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = Seq.empty
    },

    // do id and version (more complex because of cryptprovider stuff)


    // ATOMIC REPS

    SoQLText -> new SingleColumnRep(SoQLText, d"text") {
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
      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = Seq.empty
    },
    SoQLNumber -> new SingleColumnRep(SoQLNumber, Doc(SoQLFunctionSqlizerRedshift.numericType)) {
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
      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = Seq.empty

    },
    SoQLBoolean -> new SingleColumnRep(SoQLBoolean, d"boolean") {
      def literal(e: LiteralValue) = {
        val SoQLBoolean(b) = e.value
        exprSqlFactory(if(b) d"true" else d"false", e)
      }
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        val v = rs.getBoolean(dbCol)
        if(rs.wasNull) {
          SoQLNull
        } else {
          SoQLBoolean(v)
        }
      }
      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = Seq.empty
    },
    SoQLFixedTimestamp -> new SingleColumnRep(SoQLFixedTimestamp, d"timestamp with time zone") {
      def literal(e: LiteralValue) = {
        val SoQLFixedTimestamp(s) = e.value
        exprSqlFactory(sqlType +#+ mkStringLiteral(SoQLFixedTimestamp.StringRep(s)), e)
      }
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        new com.socrata.datacoordinator.common.soql.sqlreps.FixedTimestampRep("").fromResultSet(rs, dbCol)
      }
      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = Seq.empty

    },
    SoQLFloatingTimestamp -> new SingleColumnRep(SoQLFloatingTimestamp, d"timestamp without time zone") {
      def literal(e: LiteralValue) = {
        val SoQLFloatingTimestamp(s) = e.value
        exprSqlFactory(sqlType +#+ mkStringLiteral(SoQLFloatingTimestamp.StringRep(s)), e)
      }
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        new com.socrata.datacoordinator.common.soql.sqlreps.FloatingTimestampRep("").fromResultSet(rs, dbCol)
      }
      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = Seq.empty
    },
    SoQLDate -> new SingleColumnRep(SoQLDate, d"date") {
      def literal(e: LiteralValue) = {
        val SoQLDate(s) = e.value
        exprSqlFactory(sqlType +#+ mkStringLiteral(SoQLDate.StringRep(s)), e)
      }
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        new com.socrata.datacoordinator.common.soql.sqlreps.DateRep("").fromResultSet(rs, dbCol)
      }
      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = Seq.empty

    },
    SoQLTime -> new SingleColumnRep(SoQLTime, d"time without time zone") {
      def literal(e: LiteralValue) = {
        val SoQLTime(s) = e.value
        exprSqlFactory(sqlType +#+ mkStringLiteral(SoQLTime.StringRep(s)), e)
      }
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        new com.socrata.datacoordinator.common.soql.sqlreps.TimeRep("").fromResultSet(rs, dbCol)
      }
      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = Seq.empty

    },
    SoQLJson -> new SingleColumnRep(SoQLJson, d"super") { // this'll need to be super
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
      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = Seq.empty

    },

    SoQLDocument -> new SingleColumnRep(SoQLDocument, d"jsonb") { // this'll need to be a super as well
      override def literal(e: LiteralValue) = ??? // no such thing as a doc liteal
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
      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = Seq.empty
    },

    SoQLInterval -> new SingleColumnRep(SoQLInterval, d"interval") { // not even valid in redshift, apparently
      override def literal(e: LiteralValue) = {
        val SoQLInterval(p) = e.value
        exprSqlFactory(d"interval" +#+ mkStringLiteral(periodToRedshiftInterval(p)), e)
      }

      def periodToRedshiftInterval(period: Period): String = {
        var years = period.getYears
        var months = period.getMonths
        var weeks = period.getWeeks
        var days = period.getDays
        var hours = period.getHours
        var minutes = period.getMinutes
        var seconds = period.getSeconds
        var millis = period.getMillis

        val millennia = years / 1000
        years = years % 1000

        val centuries = years / 100
        years = years % 100

        val decades = years / 10
        years = years % 10

        val quarters = months / 3
        months = months % 3

        val millenniaPart = if (millennia != 0) s"$millennia millenniums" else ""
        val centuryPart = if (centuries != 0) s"$centuries centuries" else ""
        val decadePart = if (decades != 0) s"$decades decades" else ""
        val yearPart = if (years != 0) s"$years years" else ""
        val quarterPart = if (quarters != 0) s"$quarters quarters" else ""
        val monthPart = if (months != 0) s"$months months" else ""
        val weekPart = if (weeks != 0) s"$weeks weeks" else ""
        val dayPart = if (days != 0) s"$days days" else ""
        val hourPart = if (hours != 0) s"$hours hours" else ""
        val minutePart = if (minutes != 0) s"$minutes minutes" else ""
        val secondPart = if (seconds != 0) s"$seconds seconds" else ""
        val millisPart = if (millis != 0) s"$millis milliseconds" else ""

        val parts = List(millenniaPart, centuryPart, decadePart, yearPart, quarterPart, monthPart, weekPart, dayPart, hourPart, minutePart, secondPart, millisPart).filter(_.nonEmpty)
        val interval = parts.mkString(", ")

        if (interval.trim.isEmpty) {
          throw new IllegalArgumentException("Period cannot be converted to a non-empty Redshift interval")
        } else {
          interval
        }
      }
      override protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        //TODO redshiftify, interval -> period
        Option(rs.getObject(dbCol).asInstanceOf[PGInterval]) match {
          case Some(pgInterval) =>
            val period = new Period(pgInterval.getYears, pgInterval.getMonths, 0, pgInterval.getDays, pgInterval.getHours, pgInterval.getMinutes, pgInterval.getWholeSeconds, pgInterval.getMicroSeconds / 1000)
            SoQLInterval(period)
          case None =>
            SoQLNull
        }
      }
      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = Seq.empty

    },

    SoQLPoint -> new GeometryRep(SoQLPoint, SoQLPoint(_), "point") {
      override def downcast(v: SoQLValue) = v.asInstanceOf[SoQLPoint].value
    },
    SoQLMultiPoint -> new GeometryRep(SoQLMultiPoint, SoQLMultiPoint(_), "mpoint") {
      override def downcast(v: SoQLValue) = v.asInstanceOf[SoQLMultiPoint].value
      override def isPotentiallyLarge = true
    },
    SoQLLine -> new GeometryRep(SoQLLine, SoQLLine(_), "line") {
      override def downcast(v: SoQLValue) = v.asInstanceOf[SoQLLine].value
      override def isPotentiallyLarge = true
    },
    SoQLMultiLine -> new GeometryRep(SoQLMultiLine, SoQLMultiLine(_), "mline") {
      override def downcast(v: SoQLValue) = v.asInstanceOf[SoQLMultiLine].value
      override def isPotentiallyLarge = true
    },
    SoQLPolygon -> new GeometryRep(SoQLPolygon, SoQLPolygon(_), "polygon") {
      override def downcast(v: SoQLValue) = v.asInstanceOf[SoQLPolygon].value
      override def isPotentiallyLarge = true
    },
    SoQLMultiPolygon -> new GeometryRep(SoQLMultiPolygon, SoQLMultiPolygon(_), "mpoly") {
      override def downcast(v: SoQLValue) = v.asInstanceOf[SoQLMultiPolygon].value
      override def isPotentiallyLarge = true
    },

    // COMPOUND REPS

    SoQLPhone -> new CompoundColumnRep(SoQLPhone) {
      override def nullLiteral(e: NullLiteral) =
        exprSqlFactory(Seq(d"null :: text", d"null :: text"), e)

      override def expandedColumnCount = 2

      override def expandedDatabaseColumns(name: ColumnLabel) = {
        val base = namespace.columnBase(name)
        Seq(base ++ d"_number", base ++ d"_type")
      }

      override def expandedDatabaseTypes = Seq(d"text", d"text")

      override def compressedSubColumns(table: String, column: ColumnLabel) = {
        val sourceName = compressedDatabaseColumn(column)
        val Seq(provenancedName, dataName) = expandedDatabaseColumns(column)
        Seq(
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 0 AS" +#+ provenancedName, // this is all wrong
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 1 AS" +#+ dataName,
        )
      }

      override def compressedDatabaseColumn(name: ColumnLabel) =
        namespace.columnBase(name)

      override def literal(e: LiteralValue) = {
        val ph@SoQLPhone(_, _) = e.value

        ph match {
          case SoQLPhone(None, None) =>
            exprSqlFactory(d"null :: jsonb", e)
          case SoQLPhone(phNum, phTyp) =>
            val numberLit = phNum match {
              case Some(n) => mkTextLiteral(n)
              case None => d"null :: text"
            }
            val typLit = phTyp match {
              case Some(t) => mkTextLiteral(t)
              case None => d"null :: text"
            }

            exprSqlFactory(d"JSON_PARSE('[" ++ numberLit ++ d"," ++ typLit ++ d"])'", e)
        }
      }

      override def subcolInfo(field: String) =
        field match {
          case "phone_number" => SubcolInfo[MT](SoQLPhone, 0, "text", SoQLText, _.parenthesized +#+ d"->> 0")
          case "phone_type" => SubcolInfo[MT](SoQLPhone, 1, "text", SoQLText, _.parenthesized +#+ d"->> 1")
        }

      override protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        new com.socrata.datacoordinator.common.soql.sqlreps.PhoneRep("").fromResultSet(rs, dbCol)
      }
      override protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getString(dbCol)) match {
          case None =>
            SoQLNull
          case Some(v) =>
            JsonUtil.parseJson[(Either[JNull, String], Either[JNull, String])](v) match {
              case Right((Left(_), Left(_))) =>
                SoQLNull
              case Right((Left(_), Right(s))) =>
                SoQLPhone(None, Some(s))
              case Right((Right(s), Left(_))) =>
                SoQLPhone(Some(s), None)
              case Right((Right(s1), Right(s2))) =>
                SoQLPhone(Some(s1), Some(s2))
              case Left(err) =>
                throw new Exception(err.english)
            }
        }
      }
      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = Seq.empty
    },

    SoQLLocation -> new CompoundColumnRep(SoQLLocation) {
      override def physicalColumnRef(col: PhysicalColumn) = {
        val colInfo: Seq[Option[DatabaseColumnName]] = locationSubcolumns(physicalTableFor(col.table))(col.column)

        exprSqlFactory(
          (namespace.tableLabel(col.table) ++ d"." ++ compressedDatabaseColumn(col.column)) +:
            colInfo.map {
              case Some(DatabaseColumnName(dcn)) =>
                (namespace.tableLabel(col.table) ++ d"." ++ Doc(dcn))
              case None =>
                d"null :: text"
            },
          col)
      }

      override def nullLiteral(e: NullLiteral) =
        exprSqlFactory(Seq(d"null :: geometry", d"null :: text", d"null :: text", d"null :: text", d"null :: text"), e)

      override def expandedColumnCount = 5

      override def expandedDatabaseTypes = Seq(d"geometry", d"text", d"text", d"text", d"text")

      override def compressedSubColumns(table: String, column: ColumnLabel) = {
        val sourceName = compressedDatabaseColumn(column)
        val Seq(ptName, addressName, cityName, stateName, zipName) = expandedDatabaseColumns(column)
        Seq(
          d"soql_extract_compressed_location_point(" ++ Doc(table) ++ d"." ++ sourceName ++ d") AS" +#+ ptName,
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 1 AS" +#+ addressName,
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 2 AS" +#+ cityName,
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 3 AS" +#+ stateName,
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 4 AS" +#+ zipName,
        )
      }

      override def expandedDatabaseColumns(name: ColumnLabel) = {
        val base = namespace.columnBase(name)
        Seq(base, base ++ d"_address", base ++ d"_city", base ++ d"_state", base ++ d"_zip")
      }

      override def compressedDatabaseColumn(name: ColumnLabel) =
        namespace.columnBase(name)

      override def literal(e: LiteralValue) = {
        val loc@SoQLLocation(_, _, _) = e.value
        ??? // No such thing as a location literal
      }

      override def subcolInfo(field: String) =
        field match {
          case "point" => SubcolInfo[MT](SoQLLocation, 0, "geometry", SoQLPoint, { e => Seq(e.parenthesized).funcall(d"soql_extract_compressed_location_point") })
          case "address" => SubcolInfo[MT](SoQLLocation, 1, "text", SoQLText, _.parenthesized +#+ d"->> 1")
          case "city" => SubcolInfo[MT](SoQLLocation, 2, "text", SoQLText, _.parenthesized +#+ d"->> 2")
          case "state" => SubcolInfo[MT](SoQLLocation, 3, "text", SoQLText, _.parenthesized +#+ d"->> 3")
          case "zip" => SubcolInfo[MT](SoQLLocation, 4, "text", SoQLText, _.parenthesized +#+ d"->> 4")
        }

      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = Seq.empty

      override def hasTopLevelWrapper = true
      override def wrapTopLevel(raw: ExprSql) = {
        assert(raw.typ == SoQLLocation)
        raw match {
          case compressed: ExprSql.Compressed[MT] =>
            compressed
          case expanded: ExprSql.Expanded[MT] =>
            val sqls = expanded.sqls
            assert(sqls.length == expandedColumnCount)
            exprSqlFactory(Seq(sqls.head).funcall(d"st_asbinary") +: sqls.tail, expanded.expr)
        }
      }

      implicit class ToBigDecimalAugmentation(val x: Double) {
        def toBigDecimal = java.math.BigDecimal.valueOf(x)
      }

      private def locify(point: Option[Point], address: Option[String], city: Option[String], state: Option[String], zip: Option[String]): SoQLValue =
        if(address.isEmpty && city.isEmpty && state.isEmpty && zip.isEmpty) {
          if(point.isEmpty) {
            SoQLNull
          } else {
            SoQLLocation(point.map(_.getY.toBigDecimal), point.map(_.getX.toBigDecimal), None)
          }
        } else {
          SoQLLocation(
            point.map(_.getY.toBigDecimal), point.map(_.getX.toBigDecimal),
            Some(
              // Building this as a string rather than a JValue to keep the formatting exactly the same as before
              s"""{"address": ${JString(address.getOrElse(""))}, "city": ${JString(city.getOrElse(""))}, "state": ${JString(state.getOrElse(""))}, "zip": ${JString(zip.getOrElse(""))}}"""
            )
          )
        }

      override protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        locify(
          Option(rs.getBytes(dbCol)).flatMap { bytes =>
            SoQLPoint.WkbRep.unapply(bytes)
          },
          Option(rs.getString(dbCol+1)),
          Option(rs.getString(dbCol+2)),
          Option(rs.getString(dbCol+3)),
          Option(rs.getString(dbCol+4))
        )
      }

      override protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getString(dbCol)) match {
          case None =>
            SoQLNull
          case Some(v) =>
            JsonUtil.parseJson[(Either[JNull, JValue], Either[JNull, String], Either[JNull, String], Either[JNull, String], Either[JNull, String])](v) match {
              case Right((pt, address, city, state, zip)) =>
                locify(
                  pt.toOption.flatMap { ptJson => SoQLPoint.JsonRep.unapply(ptJson.toString) },
                  address.toOption,
                  city.toOption,
                  state.toOption,
                  zip.toOption
                )
              case Left(err) =>
                throw new Exception(err.english)
            }
        }
      }
    },

    SoQLUrl -> new CompoundColumnRep(SoQLUrl) {
      override def nullLiteral(e: NullLiteral) =
        exprSqlFactory(Seq(d"null :: text", d"null :: text"), e)

      override def expandedColumnCount = 2

      override def expandedDatabaseColumns(name: ColumnLabel) = {
        val base = namespace.columnBase(name)
        Seq(base ++ d"_url", base ++ d"_description")
      }

      override def expandedDatabaseTypes = Seq(d"text", d"text")

      override def compressedSubColumns(table: String, column: ColumnLabel) = {
        val sourceName = compressedDatabaseColumn(column)
        val Seq(urlName, descName) = expandedDatabaseColumns(column)
        Seq(
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 0 AS" +#+ urlName,
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 1 AS" +#+ descName,
        )
      }

      override def compressedDatabaseColumn(name: ColumnLabel) =
        namespace.columnBase(name)

      override def literal(e: LiteralValue) = {
        val url@SoQLUrl(_, _) = e.value

        url match {
          case SoQLUrl(None, None) =>
            exprSqlFactory(Seq(d"null :: text", d"null :: text"), e)
          case SoQLUrl(urlUrl, urlDesc) =>
            val urlLit = urlUrl match {
              case Some(n) => mkTextLiteral(n)
              case None => d"null :: text"
            }
            val descLit = urlDesc match {
              case Some(t) => mkTextLiteral(t)
              case None => d"null :: text"
            }

            exprSqlFactory(Seq(urlLit, descLit), e)
        }
      }

      override def subcolInfo(field: String) =
        field match {
          case "url" => SubcolInfo[MT](SoQLUrl, 0, "text", SoQLText, _.parenthesized +#+ d"->> 0")
          case "description" => SubcolInfo[MT](SoQLUrl, 1, "text", SoQLText, _.parenthesized +#+ d"->> 1")
        }

      override protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        new com.socrata.datacoordinator.common.soql.sqlreps.UrlRep("").fromResultSet(rs, dbCol)
      }
      override protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getString(dbCol)) match {
          case None =>
            SoQLNull
          case Some(v) =>
            JsonUtil.parseJson[(Either[JNull, String], Either[JNull, String])](v) match {
              case Right((Left(_), Left(_))) =>
                SoQLNull
              case Right((Left(_), Right(s))) =>
                SoQLUrl(None, Some(s))
              case Right((Right(s), Left(_))) =>
                SoQLUrl(Some(s), None)
              case Right((Right(s1), Right(s2))) =>
                SoQLUrl(Some(s1), Some(s2))
              case Left(err) =>
                throw new Exception(err.english)
            }
        }
      }
      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = Seq.empty
    }
  )
}
