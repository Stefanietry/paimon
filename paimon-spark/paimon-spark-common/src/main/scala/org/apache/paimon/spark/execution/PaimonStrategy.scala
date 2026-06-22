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

import org.apache.paimon.CoreOptions
import org.apache.paimon.fs.FileIO
import org.apache.paimon.globalindex.{GlobalIndexer, GlobalIndexerFactoryUtils, GlobalIndexIOMeta, GlobalIndexReader, GlobalIndexReadThreadPool, GlobalIndexResult, OffsetGlobalIndexReader, ScoredGlobalIndexResult}
import org.apache.paimon.globalindex.io.GlobalIndexFileReader
import org.apache.paimon.index.IndexPathFactory
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.predicate.VectorSearch
import org.apache.paimon.spark.{PaimonRecordReaderIterator, SparkCatalog, SparkGenericCatalog, SparkTable, SparkUtils}
import org.apache.paimon.spark.catalog.{SparkBaseCatalog, SupportView}
import org.apache.paimon.spark.catalyst.analysis.ResolvedPaimonView
import org.apache.paimon.spark.catalyst.plans.logical.{CopyIntoLocationCommand, CopyIntoLocationSource, CopyIntoTableCommand, CreateOrReplaceTagCommand, CreatePaimonView, DeleteTagCommand, DropPaimonView, LateralVectorSearch, PaimonCallCommand, PaimonDropPartitions, RenameTagCommand, ResolvedIdentifier, ShowPaimonViews, ShowTagsCommand, TruncatePaimonTableWithFilter}
import org.apache.paimon.spark.data.SparkInternalRow
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.{FileStoreTable, InnerTable, SpecialFields, Table}
import org.apache.paimon.table.source.{InnerTableScan, ReadBuilder, VectorScan, VectorSearchSplit}
import org.apache.paimon.types.DataField
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.{IOUtils, RoaringNavigableMap64}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
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

import java.util.Arrays
import java.util.Collections
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
  extends SparkPlan
  with Logging {

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
        lazy val searchContext = createSearchContext(rightProjection)
        var searchRuntime: LateralVectorSearchRuntime = null
        val batchSize = searchContext.batchSize
        var batchNumber = 0L
        var totalOuterRowCount = 0L
        var totalQueryCount = 0L
        val totalStartMillis = System.currentTimeMillis()
        Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit] {
          _ =>
            if (searchRuntime != null) {
              searchRuntime.close()
            }
            val totalElapsedSeconds = (System.currentTimeMillis() - totalStartMillis) / 1000.0
            logInfo(
              s"Finished lateral vector search, " +
                s"batches: $batchNumber, " +
                s"outer rows: $totalOuterRowCount, " +
                s"query vectors: $totalQueryCount, " +
                s"elapsed time: $totalElapsedSeconds s.")
        })

        // Spark input iterators may reuse the same mutable InternalRow instance for every record.
        // Copy rows before grouping, otherwise a whole batch can hold references to the final
        // input row and all lateral search results will be joined with that row.
        outerRows.map(_.copy()).grouped(batchSize).flatMap {
          outerRowBatch =>
            batchNumber += 1
            val startMillis = System.currentTimeMillis()
            val searchBatch = ArrayBuffer[LateralVectorSearchQuery]()
            outerRowBatch.foreach {
              outerRow =>
                toFloatArray(queryVectorProjection(outerRow).get(0, strippedQueryExpr.dataType))
                  .foreach(
                    queryVector => searchBatch += LateralVectorSearchQuery(outerRow, queryVector))
            }
            totalOuterRowCount += outerRowBatch.size
            totalQueryCount += searchBatch.size

            val resultIterator = if (searchBatch.isEmpty) {
              Iterator.empty
            } else {
              if (searchRuntime == null) {
                searchRuntime = new LateralVectorSearchRuntime(searchContext)
              }
              search(searchBatch, searchContext, searchRuntime).map {
                case (outerRow, rightRow) =>
                  joinedRow(outerRow, rightRow)
                  joinedRow.copy()
              }
            }
            val elapsedSeconds = (System.currentTimeMillis() - startMillis) / 1000.0
            logInfo(
              s"Finished lateral vector search batch $batchNumber, " +
                s"outer rows: ${outerRowBatch.size}, " +
                s"query vectors: ${searchBatch.size}, " +
                s"elapsed time: $elapsedSeconds s.")
            resultIterator
        }
    }
  }

  private def createSearchContext(rightProjection: UnsafeProjection): LateralVectorSearchContext = {
    val rowType = innerTable.rowType()
    val readFieldNames = vectorSearchOutput
      .filterNot(attr => vectorSearchMetadataColumn(attr.name))
      .map(_.name)
    val readFieldNamesWithRowId =
      if (readFieldNames.contains(SpecialFields.ROW_ID.name())) {
        readFieldNames
      } else {
        readFieldNames :+ SpecialFields.ROW_ID.name()
      }
    val rowTypeWithRowId = SpecialFields.rowTypeWithRowId(rowType)
    val readRowType = rowType.project(readFieldNames.asJava)
    val readRowTypeWithRowId = SpecialFields.rowTypeWithRowId(readRowType)
    val readBuilder = innerTable
      .newReadBuilder()
      .withReadType(rowTypeWithRowId.project(readFieldNamesWithRowId.asJava))
    val scoreMetadataColumns =
      if (vectorSearchOutput.exists(_.name == PaimonMetadataColumn.VECTOR_SEARCH_SCORE_COLUMN)) {
        Seq(PaimonMetadataColumn.VECTOR_SEARCH_SCORE)
      } else {
        Seq.empty
      }
    val resultRowType =
      if (scoreMetadataColumns.isEmpty) {
        readRowTypeWithRowId
      } else {
        new RowType(
          (readRowTypeWithRowId.getFields.asScala ++ scoreMetadataColumns.map(
            _.toPaimonDataField)).asJava)
      }
    val sparkRow = SparkInternalRow.create(resultRowType)
    val vectorSearchBuilder = innerTable
      .newVectorSearchBuilder()
      .withVectorColumn(columnName)
      .withLimit(limit)

    val vectorPlan = vectorSearchBuilder.newVectorScan().scan()
    val batchSize =
      Math.max(1, new CoreOptions(innerTable.options()).vectorSearchLateralJoinBatchSize())

    LateralVectorSearchContext(
      readBuilder,
      vectorPlan,
      scoreMetadataColumns,
      sparkRow,
      rowIdOrdinal = resultRowType.getFieldIndex(SpecialFields.ROW_ID.name()),
      projectionInputOrdinals = vectorSearchOutput.map {
        attr =>
          if (attr.name == PaimonMetadataColumn.VECTOR_SEARCH_SCORE_COLUMN) {
            -1
          } else {
            resultRowType.getFieldIndex(attr.name)
          }
      },
      rightProjection,
      batchSize,
      canSkipBackLookup = vectorSearchOutput.forall(attr => vectorSearchMetadataColumn(attr.name))
    )
  }

  private def search(
      queries: Seq[LateralVectorSearchQuery],
      context: LateralVectorSearchContext,
      runtime: LateralVectorSearchRuntime): Iterator[(InternalRow, InternalRow)] = {
    val vectors = queries.map(_.queryVector).asJava
    val vectorSearchStartMillis = System.currentTimeMillis()
    val globalIndexResults = runtime.readBatch(vectors).asScala
    val vectorSearchElapsedSeconds = (System.currentTimeMillis() - vectorSearchStartMillis) / 1000.0
    logInfo(
      s"Finished lateral vector search read batch, " +
        s"queries: ${queries.size}, " +
        s"elapsed time: $vectorSearchElapsedSeconds s.")
    val rowIdToMatches = createRowIdToMatches(queries, globalIndexResults)
    if (context.canSkipBackLookup) {
      return rowIdToMatches.valuesIterator.flatten.map {
        searchMatch => (searchMatch.outerRow, projectMetadataRow(searchMatch, context))
      }
    }
    val batchGlobalIndexResult = createBatchGlobalIndexResult(globalIndexResults)
    val scan = context.readBuilder
      .newScan()
      .withGlobalIndexResult(batchGlobalIndexResult)
      .asInstanceOf[InnerTableScan]
    val read = context.readBuilder.newRead()

    scan.plan().splits().asScala.iterator.flatMap {
      split =>
        val reader =
          PaimonRecordReaderIterator(read.createReader(split), context.scoreMetadataColumns, split)
        new Iterator[Iterator[(InternalRow, InternalRow)]] {
          private var closed = false

          override def hasNext: Boolean = {
            val hasNext = reader.hasNext
            if (!hasNext && !closed) {
              reader.close()
              closed = true
            }
            hasNext
          }

          override def next(): Iterator[(InternalRow, InternalRow)] = {
            val rightRow = context.sparkRow.replace(reader.next())
            val rowId = rightRow.getLong(context.rowIdOrdinal)
            rowIdToMatches.getOrElse(rowId, Seq.empty).iterator.map {
              searchMatch =>
                val projectedRow = projectRightRow(rightRow, searchMatch, context)
                (searchMatch.outerRow, projectedRow)
            }
          }
        }.flatMap(identity)
    }
  }

  private def projectRightRow(
      rightRow: InternalRow,
      searchMatch: LateralVectorSearchMatch,
      context: LateralVectorSearchContext): InternalRow = {
    val values = new Array[Any](vectorSearchOutput.size)
    vectorSearchOutput.zipWithIndex.foreach {
      case (attr, index) =>
        val ordinal = context.projectionInputOrdinals(index)
        values(index) = if (ordinal < 0) {
          searchMatch.score
        } else {
          rightRow.get(ordinal, attr.dataType)
        }
    }
    context.rightProjection(new GenericInternalRow(values))
  }

  private def projectMetadataRow(
      searchMatch: LateralVectorSearchMatch,
      context: LateralVectorSearchContext): InternalRow = {
    val values = new Array[Any](vectorSearchOutput.size)
    vectorSearchOutput.zipWithIndex.foreach {
      case (attr, index) =>
        values(index) = attr.name match {
          case PaimonMetadataColumn.ROW_ID_COLUMN => searchMatch.rowId
          case PaimonMetadataColumn.VECTOR_SEARCH_SCORE_COLUMN => searchMatch.score
        }
    }
    context.rightProjection(new GenericInternalRow(values))
  }

  private def createRowIdToMatches(
      queries: Seq[LateralVectorSearchQuery],
      globalIndexResults: Seq[GlobalIndexResult]): Map[Long, Seq[LateralVectorSearchMatch]] = {
    val rowIdToMatches =
      scala.collection.mutable.LinkedHashMap[Long, ArrayBuffer[LateralVectorSearchMatch]]()
    queries.zip(globalIndexResults).foreach {
      case (query, result) =>
        val scoreGetter = result match {
          case scored: ScoredGlobalIndexResult => Some(scored.scoreGetter())
          case _ => None
        }
        result.results().iterator().asScala.foreach {
          rowId =>
            rowIdToMatches.getOrElseUpdate(rowId, ArrayBuffer()) +=
              LateralVectorSearchMatch(
                rowId,
                query.outerRow,
                scoreGetter.map(_.score(rowId)).getOrElse(Float.NaN))
        }
    }
    rowIdToMatches.mapValues(_.toSeq).toMap
  }

  private def createBatchGlobalIndexResult(
      globalIndexResults: Seq[GlobalIndexResult]): GlobalIndexResult = {
    val rowIds = new RoaringNavigableMap64()
    val rowIdToScore = scala.collection.mutable.HashMap[Long, Float]()
    globalIndexResults.foreach {
      result =>
        rowIds.or(result.results())
        result match {
          case scored: ScoredGlobalIndexResult =>
            result
              .results()
              .iterator()
              .asScala
              .foreach(rowId => rowIdToScore.put(rowId, scored.scoreGetter().score(rowId)))
          case _ =>
        }
    }
    if (rowIdToScore.isEmpty) {
      GlobalIndexResult.create(rowIds)
    } else {
      ScoredGlobalIndexResult.create(rowIds, rowId => rowIdToScore.getOrElse(rowId, Float.NaN))
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

  private def vectorSearchMetadataColumn(name: String): Boolean = {
    name == PaimonMetadataColumn.ROW_ID_COLUMN ||
    name == PaimonMetadataColumn.VECTOR_SEARCH_SCORE_COLUMN
  }

  private class LateralVectorSearchRuntime(context: LateralVectorSearchContext)
    extends AutoCloseable {

    private val table = innerTable.asInstanceOf[FileStoreTable]
    private lazy val vectorColumn: DataField = table.rowType().getField(columnName)
    private val splits = context.vectorPlan.splits()
    private lazy val indexType = splits.get(0).vectorIndexFiles().get(0).indexType()
    private lazy val globalIndexer: GlobalIndexer = GlobalIndexerFactoryUtils
      .load(indexType)
      .create(vectorColumn, table.coreOptions().toConfiguration())
    private val indexPathFactory: IndexPathFactory =
      table.store().pathFactory().globalIndexFileFactory()
    private val fileIO: FileIO = table.fileIO()
    private val parallelism =
      table.coreOptions().toConfiguration().get(CoreOptions.GLOBAL_INDEX_THREAD_NUM)
    private val executor: ExecutorService =
      GlobalIndexReadThreadPool.getExecutorService(parallelism)
    private val maxInFlight = Math.max(1, parallelism)
    private val localReaders =
      mutable.HashMap[LateralVectorSearchReaderCacheKey, BorrowedLateralVectorSearchReader]()

    private var closed = false

    def readBatch(vectors: java.util.List[Array[Float]]): java.util.List[GlobalIndexResult] = {
      val emptyResults = new java.util.ArrayList[GlobalIndexResult](vectors.size())
      for (_ <- 0 until vectors.size()) {
        emptyResults.add(GlobalIndexResult.createEmpty())
      }
      if (vectors.isEmpty || splits.isEmpty) {
        return emptyResults
      }

      val scoredResults = new java.util.ArrayList[ScoredGlobalIndexResult](vectors.size())
      for (_ <- 0 until vectors.size()) {
        scoredResults.add(ScoredGlobalIndexResult.createEmpty())
      }

      val futures = new java.util.ArrayList[CompletableFuture[
        java.util.List[Optional[ScoredGlobalIndexResult]]]](maxInFlight)
      splits.asScala.foreach {
        split =>
          futures.add(evalBatch(split, vectors))
          if (futures.size() >= maxInFlight) {
            mergeBatchResults(scoredResults, futures)
            futures.clear()
          }
      }
      mergeBatchResults(scoredResults, futures)

      val results = new java.util.ArrayList[GlobalIndexResult](vectors.size())
      for (i <- 0 until scoredResults.size()) {
        results.add(scoredResults.get(i).topK(limit))
      }
      results
    }

    private def evalBatch(split: VectorSearchSplit, vectors: java.util.List[Array[Float]])
        : CompletableFuture[java.util.List[Optional[ScoredGlobalIndexResult]]] = {
      val borrowedReader = readerForSplit(split)
      val vectorSearches = new java.util.ArrayList[VectorSearch](vectors.size())
      for (i <- 0 until vectors.size()) {
        vectorSearches.add(
          new VectorSearch(
            vectors.get(i),
            limit,
            columnName,
            Collections.emptyMap[String, String]()))
      }

      new OffsetGlobalIndexReader(borrowedReader.reader, split.rowRangeStart(), split.rowRangeEnd())
        .visitVectorSearchBatch(vectorSearches)
    }

    private def mergeBatchResults(
        scoredResults: java.util.List[ScoredGlobalIndexResult],
        futures: java.util.List[CompletableFuture[
          java.util.List[Optional[ScoredGlobalIndexResult]]]]): Unit = {
      if (futures.isEmpty) {
        return
      }

      CompletableFuture.allOf(futures.asScala.toArray: _*).join()
      futures.asScala.foreach {
        future =>
          val next = future.join()
          for (i <- 0 until next.size()) {
            if (next.get(i).isPresent) {
              scoredResults.set(i, scoredResults.get(i).or(next.get(i).get()).topK(limit))
            }
          }
      }
    }

    private def readerForSplit(split: VectorSearchSplit): BorrowedLateralVectorSearchReader = {
      val key = readerCacheKey(split)
      localReaders.getOrElseUpdate(
        key,
        LateralVectorSearchExecutorReaderCache
          .borrow(key)
          .getOrElse(BorrowedLateralVectorSearchReader(key, createReader(split))))
    }

    private def createReader(split: VectorSearchSplit): GlobalIndexReader = {
      val indexIOMetas = new java.util.ArrayList[GlobalIndexIOMeta](split.vectorIndexFiles().size())
      split.vectorIndexFiles().asScala.foreach {
        indexFile =>
          val meta = indexFile.globalIndexMeta()
          indexIOMetas.add(
            new GlobalIndexIOMeta(
              indexPathFactory.toPath(indexFile),
              indexFile.fileSize(),
              meta.indexMeta()))
      }
      val indexFileReader: GlobalIndexFileReader =
        (meta: GlobalIndexIOMeta) => fileIO.newInputStream(meta.filePath())
      globalIndexer.createReader(indexFileReader, indexIOMetas, executor)
    }

    private def readerCacheKey(split: VectorSearchSplit): LateralVectorSearchReaderCacheKey = {
      val paths = ArrayBuffer[String]()
      val fileSizes = ArrayBuffer[Long]()
      val metadataHashes = ArrayBuffer[Int]()
      split.vectorIndexFiles().asScala.foreach {
        indexFile =>
          val meta = indexFile.globalIndexMeta()
          paths += indexPathFactory.toPath(indexFile).toString
          fileSizes += indexFile.fileSize()
          metadataHashes += Arrays.hashCode(meta.indexMeta())
      }
      LateralVectorSearchReaderCacheKey(
        indexType,
        paths.toSeq,
        fileSizes.toSeq,
        metadataHashes.toSeq)
    }

    override def close(): Unit = {
      if (!closed) {
        closed = true
        localReaders.values.foreach(LateralVectorSearchExecutorReaderCache.release)
        localReaders.clear()
      }
    }
  }

  private case class LateralVectorSearchContext(
      readBuilder: ReadBuilder,
      vectorPlan: VectorScan.Plan,
      scoreMetadataColumns: Seq[PaimonMetadataColumn],
      sparkRow: SparkInternalRow,
      rowIdOrdinal: Int,
      projectionInputOrdinals: Seq[Int],
      rightProjection: UnsafeProjection,
      batchSize: Int,
      canSkipBackLookup: Boolean)

  private case class LateralVectorSearchQuery(outerRow: InternalRow, queryVector: Array[Float])

  private case class LateralVectorSearchMatch(rowId: Long, outerRow: InternalRow, score: Float)
}

private case class LateralVectorSearchReaderCacheKey(
    indexType: String,
    paths: Seq[String],
    fileSizes: Seq[Long],
    metadataHashes: Seq[Int])

private case class BorrowedLateralVectorSearchReader(
    key: LateralVectorSearchReaderCacheKey,
    reader: GlobalIndexReader)

private object LateralVectorSearchExecutorReaderCache {

  private val MaxCachedReaders = 64
  private val cachedReaders =
    mutable.LinkedHashMap[LateralVectorSearchReaderCacheKey, GlobalIndexReader]()

  def borrow(key: LateralVectorSearchReaderCacheKey): Option[BorrowedLateralVectorSearchReader] =
    synchronized {
      cachedReaders.remove(key).map(BorrowedLateralVectorSearchReader(key, _))
    }

  def release(borrowedReader: BorrowedLateralVectorSearchReader): Unit = synchronized {
    if (cachedReaders.contains(borrowedReader.key)) {
      IOUtils.closeQuietly(borrowedReader.reader)
      return
    }
    if (cachedReaders.size >= MaxCachedReaders) {
      val oldestKey = cachedReaders.head._1
      val oldestReader = cachedReaders.remove(oldestKey).get
      IOUtils.closeQuietly(oldestReader)
    }
    cachedReaders.put(borrowedReader.key, borrowedReader.reader)
  }
}
