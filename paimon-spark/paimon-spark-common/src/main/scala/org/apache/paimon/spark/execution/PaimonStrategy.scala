/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.execution

import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.spark.{PaimonRecordReaderIterator, SparkCatalog, SparkGenericCatalog, SparkTable, SparkUtils}
import org.apache.paimon.spark.catalog.{SparkBaseCatalog, SupportView}
import org.apache.paimon.spark.catalyst.analysis.ResolvedPaimonView
import org.apache.paimon.spark.catalyst.plans.logical.{CopyIntoLocationCommand, CopyIntoLocationSource, CopyIntoTableCommand, CreateOrReplaceTagCommand, CreatePaimonView, DeleteTagCommand, DropPaimonView, LateralVectorSearch, PaimonCallCommand, PaimonDropPartitions, RenameTagCommand, ResolvedIdentifier, ShowPaimonViews, ShowTagsCommand, TruncatePaimonTableWithFilter}
import org.apache.paimon.spark.data.SparkInternalRow
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.{InnerTable, Table}
import org.apache.paimon.table.source.{InnerTableScan, ReadBuilder, VectorSearchBuilder}
import org.apache.paimon.types.RowType

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ResolvedNamespace, ResolvedTable}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, GenericInternalRow, JoinedRow, NamedExpression, OuterReference, PredicateHelper, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, DescribeRelation, LogicalPlan, ReplaceTable, ReplaceTableAsSelect, ShowCreateTable}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.catalog.{Identifier, PaimonLookupCatalog, TableCatalog}
import org.apache.spark.sql.execution.{PaimonDescribeTableExec, SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Implicits, DataSourceV2Relation}
import org.apache.spark.sql.execution.shim.{PaimonCreateTableAsSelectStrategy, PaimonReplaceTableAsSelectStrategy, PaimonReplaceTableStrategy}
import org.apache.spark.sql.paimon.shims.SparkShimLoader

import scala.collection.JavaConverters._

case class PaimonStrategy(spark: SparkSession)
  extends SparkStrategy
  with PredicateHelper
  with PaimonLookupCatalog {

  import DataSourceV2Implicits._
  protected lazy val catalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case ctas: CreateTableAsSelect =>
      PaimonCreateTableAsSelectStrategy(spark)(ctas)

    case rtas: ReplaceTableAsSelect =>
      PaimonReplaceTableAsSelectStrategy(spark)(rtas)

    case rt: ReplaceTable =>
      PaimonReplaceTableStrategy(spark)(rt)

    case c @ PaimonCallCommand(procedure, args) =>
      val input = buildInternalRow(args)
      PaimonCallExec(c.output, procedure, input) :: Nil

    case lvs: LateralVectorSearch =>
      LateralVectorSearchExec(
        lvs.innerTable,
        lvs.columnName,
        lvs.queryVectorExpr,
        lvs.limit,
        lvs.vectorSearchOutput,
        lvs.projectList,
        lvs.projectOutput,
        planLater(lvs.left)) :: Nil

    case t @ ShowTagsCommand(PaimonCatalogAndIdentifier(catalog, ident)) =>
      ShowTagsExec(catalog, ident, t.output) :: Nil

    case CreateOrReplaceTagCommand(
          PaimonCatalogAndIdentifier(table, ident),
          tagName,
          tagOptions,
          create,
          replace,
          ifNotExists) =>
      CreateOrReplaceTagExec(table, ident, tagName, tagOptions, create, replace, ifNotExists) :: Nil

    case DeleteTagCommand(PaimonCatalogAndIdentifier(catalog, ident), tagStr, ifExists) =>
      DeleteTagExec(catalog, ident, tagStr, ifExists) :: Nil

    case RenameTagCommand(PaimonCatalogAndIdentifier(catalog, ident), sourceTag, targetTag) =>
      RenameTagExec(catalog, ident, sourceTag, targetTag) :: Nil

    case CreatePaimonView(
          ResolvedIdentifier(viewCatalog: SupportView, ident),
          queryText,
          query,
          columnAliases,
          columnComments,
          queryColumnNames,
          comment,
          properties,
          allowExisting,
          replace
        ) =>
      CreatePaimonViewExec(
        viewCatalog,
        ident,
        queryText,
        query.schema,
        columnAliases,
        columnComments,
        queryColumnNames,
        comment,
        properties,
        allowExisting,
        replace) :: Nil

    case DropPaimonView(ResolvedIdentifier(viewCatalog: SupportView, ident), ifExists) =>
      DropPaimonViewExec(viewCatalog, ident, ifExists) :: Nil

    // A new member was added to ResolvedNamespace since spark4.0,
    // unapply pattern matching is not used here to ensure compatibility across multiple spark versions.
    case ShowPaimonViews(r: ResolvedNamespace, pattern, output)
        if r.catalog.isInstanceOf[SupportView] =>
      ShowPaimonViewsExec(output, r.catalog.asInstanceOf[SupportView], r.namespace, pattern) :: Nil

    case ShowCreateTable(ResolvedPaimonView(viewCatalog, ident), _, output) =>
      ShowCreatePaimonViewExec(output, viewCatalog, ident) :: Nil

    case DescribeRelation(ResolvedPaimonView(viewCatalog, ident), _, isExtended, output) =>
      DescribePaimonViewExec(output, viewCatalog, ident, isExtended) :: Nil

    case DescribeRelation(r: ResolvedTable, partitionSpec, isExtended, output) =>
      (r.table, r.catalog) match {
        case (sparkTable: SparkTable, sparkCatalog: SparkBaseCatalog) =>
          PaimonDescribeTableExec(
            output,
            sparkCatalog,
            r.identifier,
            sparkTable,
            partitionSpec,
            isExtended) :: Nil
        case _ => Nil
      }

    case PaimonDropPartitions(
          r @ ResolvedTable(_, _, table: SparkTable, _),
          parts,
          ifExists,
          purge) =>
      PaimonDropPartitionsExec(
        table,
        parts.asResolvedPartitionSpecs,
        ifExists,
        purge,
        recacheTable(r)) :: Nil

    case TruncatePaimonTableWithFilter(
          table: Table,
          partitionPredicate: Option[PartitionPredicate]) =>
      TruncatePaimonTableWithFilterExec(table, partitionPredicate) :: Nil

    case c @ CopyIntoTableCommand(PaimonCatalogAndIdentifier(catalog, ident), _, _, _, _, _, _) =>
      CopyIntoTableExec(
        spark,
        catalog,
        ident,
        c.sourcePath,
        c.columns,
        c.fileFormat,
        c.pattern,
        c.force,
        c.onError,
        c.output) :: Nil

    case c @ CopyIntoLocationCommand(_, CopyIntoLocationSource.Query(query), _, _) =>
      CopyIntoLocationExec(
        spark,
        CopyIntoSource.QuerySource(query),
        c.targetPath,
        c.fileFormat,
        c.overwrite,
        c.output) :: Nil

    case c @ CopyIntoLocationCommand(
          _,
          CopyIntoLocationSource.TableName(PaimonCatalogAndIdentifier(catalog, ident)),
          _,
          _) =>
      CopyIntoLocationExec(
        spark,
        CopyIntoSource.TableSource(catalog, ident),
        c.targetPath,
        c.fileFormat,
        c.overwrite,
        c.output) :: Nil

    case _ => Nil
  }

  private def buildInternalRow(exprs: Seq[Expression]): InternalRow = {
    val values = new Array[Any](exprs.size)
    for (index <- exprs.indices) {
      values(index) = exprs(index).eval()
    }
    new GenericInternalRow(values)
  }

  private object PaimonCatalogAndIdentifier {
    def unapply(identifier: Seq[String]): Option[(TableCatalog, Identifier)] = {
      val catalogAndIdentifier =
        SparkUtils.catalogAndIdentifier(spark, identifier.asJava, catalogManager.currentCatalog)
      catalogAndIdentifier.catalog match {
        case paimonCatalog: SparkCatalog =>
          Some((paimonCatalog, catalogAndIdentifier.identifier()))
        case paimonCatalog: SparkGenericCatalog =>
          Some((paimonCatalog, catalogAndIdentifier.identifier()))
        case _ =>
          None
      }
    }
  }

  private def recacheTable(r: ResolvedTable)(): Unit = {
    val v2Relation = DataSourceV2Relation.create(r.table, Some(r.catalog), Some(r.identifier))
    SparkShimLoader.shim.classicApi.recacheByPlan(spark, v2Relation)
  }
}

case class LateralVectorSearchExec(
    innerTable: InnerTable,
    columnName: String,
    queryVectorExpr: Expression,
    limit: Int,
    vectorSearchOutput: Seq[Attribute],
    projectList: Seq[NamedExpression],
    projectOutput: Seq[Attribute],
    child: SparkPlan)
  extends SparkPlan {

  override def children: Seq[SparkPlan] = Seq(child)

  override def output: Seq[Attribute] = child.output ++ projectOutput

  @transient override lazy val producedAttributes: AttributeSet = AttributeSet(vectorSearchOutput)

  @transient
  override lazy val references: AttributeSet = {
    AttributeSet.fromAttributeSets(expressions.map(_.references)) -- producedAttributes
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    copy(child = newChildren.head)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions {
      outerRows =>
        val strippedQueryExpr = queryVectorExpr.transform {
          case OuterReference(namedExpression) => namedExpression.toAttribute
        }
        val queryVectorProjection = UnsafeProjection.create(Seq(strippedQueryExpr), child.output)
        val rightProjection = UnsafeProjection.create(projectList, vectorSearchOutput)
        val joinedRow = new JoinedRow
        lazy val searchContext = createSearchContext()

        outerRows.flatMap {
          outerRow =>
            toFloatArray(queryVectorProjection(outerRow).get(0, strippedQueryExpr.dataType)) match {
              case Some(queryVector) =>
                search(queryVector, searchContext).map {
                  rightRow =>
                    joinedRow(outerRow, rightProjection(rightRow))
                    joinedRow.copy()
                }
              case None => Iterator.empty
            }
        }
    }
  }

  private def createSearchContext(): LateralVectorSearchContext = {
    val rowType = innerTable.rowType()
    val readFieldNames = vectorSearchOutput
      .filterNot(_.name == PaimonMetadataColumn.VECTOR_SEARCH_SCORE_COLUMN)
      .map(_.name)
    val readRowType = rowType.project(readFieldNames.asJava)
    val readBuilder = innerTable.newReadBuilder().withReadType(readRowType)
    val scoreMetadataColumns =
      if (vectorSearchOutput.exists(_.name == PaimonMetadataColumn.VECTOR_SEARCH_SCORE_COLUMN)) {
        Seq(PaimonMetadataColumn.VECTOR_SEARCH_SCORE)
      } else {
        Seq.empty
      }
    val resultRowType =
      if (scoreMetadataColumns.isEmpty) {
        readRowType
      } else {
        new RowType(
          (readRowType.getFields.asScala ++ scoreMetadataColumns.map(_.toPaimonDataField)).asJava)
      }
    val sparkRow = SparkInternalRow.create(resultRowType)
    val vectorSearchBuilder = innerTable
      .newVectorSearchBuilder()
      .withVectorColumn(columnName)
      .withLimit(limit)

    LateralVectorSearchContext(readBuilder, vectorSearchBuilder, scoreMetadataColumns, sparkRow)
  }

  private def search(
      queryVector: Array[Float],
      context: LateralVectorSearchContext): Iterator[InternalRow] = {
    val globalIndexResult = context.vectorSearchBuilder.withVector(queryVector).executeLocal()
    val scan = context.readBuilder
      .newScan()
      .withGlobalIndexResult(globalIndexResult)
      .asInstanceOf[InnerTableScan]
    val read = context.readBuilder.newRead()

    scan.plan().splits().asScala.iterator.flatMap {
      split =>
        val reader =
          PaimonRecordReaderIterator(read.createReader(split), context.scoreMetadataColumns, split)
        new Iterator[InternalRow] {
          private var closed = false

          override def hasNext: Boolean = {
            val hasNext = reader.hasNext
            if (!hasNext && !closed) {
              reader.close()
              closed = true
            }
            hasNext
          }

          override def next(): InternalRow = {
            context.sparkRow.replace(reader.next()).copy()
          }
        }
    }
  }

  private def toFloatArray(value: Any): Option[Array[Float]] = {
    value match {
      case null => None
      case arrayData: ArrayData => Some(arrayData.toFloatArray())
      case _ =>
        throw new RuntimeException(s"Cannot extract query vector from expression value: $value")
    }
  }

  private case class LateralVectorSearchContext(
      readBuilder: ReadBuilder,
      vectorSearchBuilder: VectorSearchBuilder,
      scoreMetadataColumns: Seq[PaimonMetadataColumn],
      sparkRow: SparkInternalRow)
}
