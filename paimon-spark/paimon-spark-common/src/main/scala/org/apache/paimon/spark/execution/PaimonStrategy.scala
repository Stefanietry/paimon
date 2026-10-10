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

import org.apache.paimon.{CoreOptions, Snapshot}
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.fs.Path
import org.apache.paimon.globalindex.{GlobalIndexResult, IndexedSplit, ScoredGlobalIndexResult}
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.partition.PartitionPredicate.splitPartitionPredicatesAndDataPredicates
import org.apache.paimon.predicate.{Predicate, PredicateBuilder}
import org.apache.paimon.spark.{PaimonRecordReaderIterator, PaimonScan, PostponeMergeInputScan, PostponeMergeOnRead, SparkCatalog, SparkGenericCatalog, SparkTable, SparkUtils}
import org.apache.paimon.spark.catalog.{SparkBaseCatalog, SupportView}
import org.apache.paimon.spark.catalyst.analysis.ResolvedPaimonView
import org.apache.paimon.spark.catalyst.optimizer.PushDownMapSelectedKeys
import org.apache.paimon.spark.catalyst.optimizer.RepartitionLateralVectorSearchInput
import org.apache.paimon.spark.catalyst.plans.logical.{CopyIntoLocationCommand, CopyIntoLocationSource, CopyIntoTableCommand, CreateOrReplaceTagCommand, CreatePaimonView, DeleteTagCommand, DropPaimonView, LateralVectorSearch, PaimonCallCommand, PaimonDropPartitions, PaimonTableValuedFunctions, PartitionBasedLateralVectorSearch, RenameTagCommand, ResolvedIdentifier, ShowPaimonViews, ShowTagsCommand, TruncatePaimonTableWithFilter}
import org.apache.paimon.spark.data.SparkInternalRow
import org.apache.paimon.spark.format.PaimonFormatTable
import org.apache.paimon.spark.read.VectorSearchResultUtils
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.{FileStoreTable, InnerTable, SpecialFields, Table}
import org.apache.paimon.table.source.{BatchVectorSearchBuilder, DataSplit, InnerTableScan, PrimaryKeyScoredResult, PrimaryKeySearchPosition, PrimaryKeyVectorResult, ReadBuilder, VectorScan}
import org.apache.paimon.table.source.snapshot.TimeTravelUtil
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.{IOUtils, RoaringNavigableMap64}

import org.apache.spark.{HashPartitioner, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ResolvedNamespace, ResolvedTable}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, Expression, GenericInternalRow, JoinedRow, PredicateHelper, UnsafeProjection}
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{AddPartitions, CreateTableAsSelect, DescribeRelation, DropPartitions, LogicalPlan, RepairTable, ReplaceTable, ReplaceTableAsSelect, ShowCreateTable}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.catalog.{Identifier, PaimonLookupCatalog, TableCatalog}
import org.apache.spark.sql.execution.{FilterExec, GlobalLimitExec, LeafExecNode, PaimonDescribeTableExec, ProjectExec, SparkPlan, SparkStrategy, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Implicits, DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.execution.shim.{PaimonCreateTableAsSelectStrategy, PaimonReplaceTableAsSelectStrategy, PaimonReplaceTableStrategy}
import org.apache.spark.sql.paimon.shims.SparkShimLoader

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class PaimonStrategy(spark: SparkSession)
  extends SparkStrategy
  with PredicateHelper
  with PaimonLookupCatalog {

  import DataSourceV2Implicits._
  protected lazy val catalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    // Spark creates DataSourceV2ScanRelation after injected optimizer rules have run. Apply this
    // rewrite during physical planning, when the scan relation is available, and let the regular
    // Spark strategies plan the rewritten logical subtree.
    val rewritten = PushDownMapSelectedKeys(plan)
    if (!rewritten.fastEquals(plan)) {
      planLater(rewritten) :: Nil
    } else {
      applyWithoutMapSelectedKeysPushDown(plan)
    }
  }

  private def applyWithoutMapSelectedKeysPushDown(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case PhysicalOperation(projects, filters, relation: DataSourceV2ScanRelation) =>
      relation.scan match {
        case scan: PaimonScan if PostponeMergeOnRead.usesCustomSource(scan.table) =>
          scan.planPostponeMerge() match {
            case Some(mergePlan) =>
              val inputScan = PostponeMergeInputScan(mergePlan)
              val inputOutput =
                org.apache.spark.sql.PaimonUtils.toAttributes(inputScan.readSchema())
              val inputRelation =
                SparkShimLoader.shim.createDataSourceV2ScanRelation(
                  relation,
                  inputScan,
                  inputOutput)
              val merge =
                PostponeMergeOnReadExec(
                  relation.output,
                  mergePlan,
                  PostponeMergeOnReadExec.computeShufflePartitions(
                    mergePlan.corePlan,
                    scan.coreOptions,
                    spark.sessionState.conf),
                  planLater(inputRelation)
                )
              val filtered =
                filters.reduceLeftOption(And).map(FilterExec(_, merge)).getOrElse(merge)
              ProjectExec(projects, filtered) :: Nil
            case None =>
              ProjectExec(projects, EmptyPostponeMergeExec(relation.output)) :: Nil
          }
        case _ => Nil
      }

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
        lvs.options,
        lvs.vectorSearchOutput,
        lvs.projectList,
        lvs.projectOutput,
        lvs.searchFilters,
        planLater(lvs.left)
      ) :: Nil

    case pblvs: PartitionBasedLateralVectorSearch =>
      new PartitionBasedLateralVectorSearchExec(
        pblvs.innerTable,
        pblvs.columnName,
        pblvs.queryVectorExpr,
        pblvs.limit,
        pblvs.options,
        pblvs.vectorSearchOutput,
        pblvs.projectList,
        pblvs.projectOutput,
        pblvs.searchFilters,
        planLater(pblvs.left)
      ) :: Nil

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
        spark,
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

    case AddPartitions(r @ ResolvedTable(_, _, table: PaimonFormatTable, _), parts, ifNotExists) =>
      PaimonAddFormatTablePartitionsExec(
        table,
        parts.asResolvedPartitionSpecs,
        ifNotExists,
        recacheTable(r)) :: Nil

    // Spark's DataSourceV2Strategy rejects RepairTable for every v2 table; Format Tables with
    // catalog-managed partitions support it through the sync engine, so intercept here (extension
    // strategies run first). Tables using filesystem partition discovery fall through and keep
    // the upstream rejection.
    case RepairTable(
          r @ ResolvedTable(_, _, table: PaimonFormatTable, _),
          enableAddPartitions,
          enableDropPartitions) if table.hasCatalogManagedPartitions =>
      PaimonRepairFormatTablePartitionsExec(
        table,
        enableAddPartitions,
        enableDropPartitions,
        recacheTable(r)) :: Nil

    case DropPartitions(
          r @ ResolvedTable(_, _, table: PaimonFormatTable, _),
          parts,
          ifExists,
          purge) =>
      PaimonDropFormatTablePartitionsExec(
        table,
        parts.asResolvedPartitionSpecs,
        ifExists,
        purge,
        recacheTable(r)) :: Nil

    case PaimonDropPartitions(
          r @ ResolvedTable(_, _, table: PaimonFormatTable, _),
          parts,
          ifExists,
          purge) =>
      PaimonDropFormatTablePartitionsExec(
        table,
        parts.asResolvedPartitionSpecs,
        ifExists,
        purge,
        recacheTable(r)) :: Nil

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

private case class EmptyPostponeMergeExec(output: Seq[Attribute]) extends LeafExecNode {

  override protected def doExecute(): RDD[InternalRow] = sparkContext.emptyRDD[InternalRow]
}

object LateralVectorSearchExec {

  private[spark] def partitionBasedRoutingOptions(
      options: Map[String, String]): Map[String, String] = {
    options
  }

  private case class PartitionBasedRoutedQuery(
      queryId: Long,
      outerRow: InternalRow,
      // Null for deterministic query expressions; recomputed from outerRow after centroid shuffle.
      queryVector: Array[Float])

  private case class PartitionBasedMatch(
      queryId: Long,
      outerRow: InternalRow,
      rowId: Long,
      score: Float)
}

case class LateralVectorSearchExec(
    innerTable: InnerTable,
    columnName: String,
    queryVectorExpr: Expression,
    limit: Int,
    options: Map[String, String],
    vectorSearchOutput: Seq[Attribute],
    projectList: Seq[Expression],
    projectOutput: Seq[Attribute],
    searchFilters: Seq[Expression],
    child: SparkPlan,
    planVectorSearchPerBatch: Boolean = false)
  extends SparkPlan
  with PredicateHelper
  with Logging {

  import LateralVectorSearchExec.{PartitionBasedMatch, PartitionBasedRoutedQuery}

  override def children: Seq[SparkPlan] = Seq(child)

  override def output: Seq[Attribute] = child.output ++ projectOutput

  // Statistics-based broadcast selection is only known after physical planning. Request a
  // distribution here so EnsureRequirements can restore the streamed LIMIT side's parallelism.
  override def requiredChildDistribution: Seq[Distribution] = {
    if (hasUnrepartitionedGlobalLimit(child)) {
      Seq(
        SparkShimLoader.shim.createClusteredDistribution(
          child.output,
          RepartitionLateralVectorSearchInput.parallelism))
    } else {
      Seq(UnspecifiedDistribution)
    }
  }

  private def hasUnrepartitionedGlobalLimit(plan: SparkPlan): Boolean = plan match {
    case _: ShuffleExchangeLike => false
    case _: GlobalLimitExec => true
    case join: BroadcastHashJoinExec =>
      hasUnrepartitionedGlobalLimit(if (join.buildSide == BuildRight) join.left else join.right)
    case join: BroadcastNestedLoopJoinExec =>
      hasUnrepartitionedGlobalLimit(if (join.buildSide == BuildRight) join.left else join.right)
    case unary: UnaryExecNode => hasUnrepartitionedGlobalLimit(unary.child)
    case _ => false
  }

  @transient override lazy val producedAttributes: AttributeSet = {
    AttributeSet(vectorSearchOutput ++ output.filterNot(attr => inputSet.contains(attr)))
  }

  @transient
  override lazy val references: AttributeSet = {
    AttributeSet.fromAttributeSets(expressions.map(_.references)) -- producedAttributes
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    copy(child = newChildren.head)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    if (planVectorSearchPerBatch) {
      return doExecutePartitionBased()
    }

    child.execute().mapPartitions {
      outerRows =>
        val queryVectorProjection = UnsafeProjection.create(Seq(queryVectorExpr), child.output)
        val rightProjection =
          UnsafeProjection.create(projectList, child.output ++ vectorSearchOutput)
        val joinedRow = new JoinedRow
        val readerTracker = new LateralVectorSearchReaderTracker
        Option(TaskContext.get())
          .foreach(_.addTaskCompletionListener[Unit](_ => readerTracker.closeCurrent()))
        val searchContext = createSearchContext(rightProjection, readerTracker)
        val batchSize = searchContext.batchSize

        outerRows.map(_.copy()).grouped(batchSize).flatMap {
          outerRowBatch =>
            val searchBatch = ArrayBuffer[LateralVectorSearchQuery]()
            outerRowBatch.foreach {
              outerRow =>
                toFloatArray(queryVectorProjection(outerRow).get(0, queryVectorExpr.dataType))
                  .foreach(
                    queryVector => searchBatch += LateralVectorSearchQuery(outerRow, queryVector))
            }

            if (searchBatch.isEmpty) {
              Iterator.empty
            } else {
              search(searchBatch.toVector, searchContext).map {
                case (outerRow, rightRow) =>
                  joinedRow(outerRow, rightRow)
                  joinedRow.copy()
              }
            }
        }
    }
  }

  private def doExecutePartitionBased(): RDD[InternalRow] = {
    val pinnedSnapshot = Option(
      TimeTravelUtil.tryTravelOrLatest(innerTable.asInstanceOf[FileStoreTable]))
    val carryQueryVectorInShuffle = !queryVectorExpr.deterministic
    val resultVectorSearchOutput = partitionBasedResultVectorSearchOutput
    val queryRows = child.execute().map(_.copy()).zipWithUniqueId().map {
      case (outerRow, queryId) => queryId -> outerRow
    }

    val routedQueries = queryRows.mapPartitions {
      outerRows =>
        val queryVectorProjection = UnsafeProjection.create(Seq(queryVectorExpr), child.output)
        val rightProjection =
          UnsafeProjection.create(projectList, child.output ++ resultVectorSearchOutput)
        val readerTracker = new LateralVectorSearchReaderTracker
        val routingModelCache = new java.util.HashMap[Path, AutoCloseable]()
        Option(TaskContext.get()).foreach(
          _.addTaskCompletionListener[Unit](
            _ => {
              readerTracker.closeCurrent()
              routingModelCache.values().asScala.foreach(model => IOUtils.closeQuietly(model))
            }))
        val searchContext = createSearchContext(
          rightProjection,
          readerTracker,
          pinnedSnapshot,
          resultVectorSearchOutput)

        outerRows.grouped(searchContext.batchSize).flatMap {
          outerRowBatch =>
            val searchBatch = outerRowBatch.flatMap {
              case (queryId, outerRow) =>
                toFloatArray(queryVectorProjection(outerRow).get(0, queryVectorExpr.dataType))
                  .map(
                    queryVector =>
                      PartitionBasedRoutedQuery(
                        queryId,
                        outerRow,
                        if (carryQueryVectorInShuffle) queryVector else null) -> queryVector)
            }

            if (searchBatch.isEmpty) {
              Iterator.empty
            } else {
              val routedCentroids = searchContext.routingVectorSearchBuilder
                .withVectors(searchBatch.map(_._2).toArray)
                .routeCentroids(routingModelCache)
              if (routedCentroids == null) {
                throw new UnsupportedOperationException(
                  "Partition-based LATERAL vector_search requires centroid-based IVF indexes.")
              }
              require(
                routedCentroids.size() == searchBatch.size,
                s"Centroid routing returned ${routedCentroids.size()} results for " +
                  s"${searchBatch.size} query vectors.")
              searchBatch.zip(routedCentroids.asScala).flatMap {
                case ((query, _), centroids) =>
                  centroids.asScala.map(centroid => centroid.intValue() -> query)
              }
            }
        }
    }

    val centroidPartitioner = new HashPartitioner(RepartitionLateralVectorSearchInput.parallelism)
    logInfo(
      "Partition-based lateral vector search shuffle schema: " +
        s"stage=route-by-centroid, key=centroid: Int, " +
        s"value=PartitionBasedRoutedQuery(queryId: Long, outerRow: InternalRow, " +
        s"queryVector: Array[Float] only for non-deterministic query expression), " +
        s"partitions=${centroidPartitioner.numPartitions}")

    val partialMatches = routedQueries
      .partitionBy(centroidPartitioner)
      .mapPartitions {
        partitionQueries =>
          val queryVectorProjection = UnsafeProjection.create(Seq(queryVectorExpr), child.output)
          val rightProjection =
            UnsafeProjection.create(projectList, child.output ++ resultVectorSearchOutput)
          val readerTracker = new LateralVectorSearchReaderTracker
          Option(TaskContext.get())
            .foreach(_.addTaskCompletionListener[Unit](_ => readerTracker.closeCurrent()))
          val searchContext = createSearchContext(
            rightProjection,
            readerTracker,
            pinnedSnapshot,
            resultVectorSearchOutput)
          val queryBuffers =
            scala.collection.mutable.HashMap.empty[Int, ArrayBuffer[PartitionBasedRoutedQuery]]

          def searchRoutedCentroidBatch(
              centroid: Int,
              queryBatch: Seq[PartitionBasedRoutedQuery]): Iterator[PartitionBasedMatch] = {
            val queryVectors = queryBatch.map(routedQueryVector(_, queryVectorProjection)).toArray
            val javaCentroids = Set(Integer.valueOf(centroid)).asJava
            val globalIndexResults = searchContext.vectorSearchBuilder
              .withRoutedCentroids(javaCentroids)
              .withVectors(queryVectors)
              .newBatchVectorRead()
              .readBatch(vectorPlan(searchContext))
              .asScala
              .toVector
            require(
              globalIndexResults.size == queryBatch.size,
              s"Local partition-based vector search returned ${globalIndexResults.size} " +
                s"results for ${queryBatch.size} query vectors."
            )
            queryBatch.iterator.zip(globalIndexResults.iterator).flatMap {
              case (query, result) =>
                val scoreGetter = result match {
                  case scored: ScoredGlobalIndexResult => scored.scoreGetter()
                  case _ =>
                    throw new UnsupportedOperationException(
                      "Partition-based LATERAL vector_search requires scored vector search results.")
                }
                result.results().iterator().asScala.map {
                  rowId =>
                    PartitionBasedMatch(
                      query.queryId,
                      query.outerRow,
                      rowId,
                      scoreGetter.score(rowId))
                }
            }
          }

          partitionQueries.flatMap {
            case (centroid, query) =>
              val buffer =
                queryBuffers.getOrElseUpdate(centroid, ArrayBuffer[PartitionBasedRoutedQuery]())
              buffer += query
              if (buffer.size >= searchContext.batchSize) {
                queryBuffers.remove(centroid)
                searchRoutedCentroidBatch(centroid, buffer.toVector)
              } else {
                Iterator.empty
              }
          } ++ queryBuffers.iterator.flatMap {
            case (centroid, queryBuffer) =>
              if (queryBuffer.isEmpty) {
                Iterator.empty
              } else {
                searchRoutedCentroidBatch(centroid, queryBuffer.toVector)
              }
          }
      }

    logInfo(
      "Partition-based lateral vector search shuffle schema: " +
        s"stage=topk-by-query-id, key=queryId: Long, " +
        s"value=PartitionBasedMatch(queryId: Long, outerRow: InternalRow, " +
        s"rowId: Long, score: Float), " +
        s"partitions=${partialMatches.partitions.length}")
    val topKMatches = partialMatches
      .map(matchResult => matchResult.queryId -> matchResult)
      .combineByKey[ArrayBuffer[PartitionBasedMatch]](
        (matchResult: PartitionBasedMatch) => ArrayBuffer[PartitionBasedMatch](matchResult),
        (matches: ArrayBuffer[PartitionBasedMatch], matchResult: PartitionBasedMatch) =>
          trimPartitionBasedMatches(matches += matchResult),
        (left: ArrayBuffer[PartitionBasedMatch], right: ArrayBuffer[PartitionBasedMatch]) =>
          trimPartitionBasedMatches(left ++= right)
      )
      .flatMapValues(matches => sortPartitionBasedMatches(matches).take(limit))

    logInfo(
      "Partition-based lateral vector search local projection schema: " +
        s"stage=project-topk, value=(outerRow: InternalRow${attributesSchema(child.output)}, " +
        s"match: PartitionBasedMatch), partitions=${topKMatches.partitions.length}")
    val topKMatchesWithOuterRows = topKMatches.map {
      case (_, matchResult) => matchResult.outerRow -> matchResult
    }

    topKMatchesWithOuterRows.mapPartitions {
      matches =>
        val matchBatch = matches.toVector
        if (matchBatch.isEmpty) {
          Iterator.empty
        } else {
          val rightProjection =
            UnsafeProjection.create(projectList, child.output ++ resultVectorSearchOutput)
          val readerTracker = new LateralVectorSearchReaderTracker
          Option(TaskContext.get())
            .foreach(_.addTaskCompletionListener[Unit](_ => readerTracker.closeCurrent()))
          val searchContext = createSearchContext(
            rightProjection,
            readerTracker,
            pinnedSnapshot,
            resultVectorSearchOutput)
          val joinedRow = new JoinedRow
          if (searchContext.metaColumnsOnly) {
            projectPartitionBasedMetaOnly(matchBatch, searchContext).map {
              case (outerRow, rightRow) =>
                joinedRow(outerRow, rightRow)
                joinedRow.copy()
            }
          } else {
            fetchPartitionBasedRows(matchBatch, searchContext).map {
              case (outerRow, rightRow) =>
                joinedRow(outerRow, rightRow)
                joinedRow.copy()
            }
          }
        }
    }
  }

  private def routedQueryVector(
      query: PartitionBasedRoutedQuery,
      queryVectorProjection: UnsafeProjection): Array[Float] = {
    if (query.queryVector != null) {
      query.queryVector
    } else {
      toFloatArray(queryVectorProjection(query.outerRow).get(0, queryVectorExpr.dataType))
        .getOrElse(throw new IllegalStateException(
          s"Cannot recompute query vector for routed query ${query.queryId}."))
    }
  }

  private lazy val partitionBasedResultVectorSearchOutput: Seq[Attribute] = {
    val projectReferences = AttributeSet.fromAttributeSets(projectList.map(_.references))
    val projectOnlyOutput = vectorSearchOutput.filter(projectReferences.contains)
    if (
      VectorSearchResultUtils.isVectorSearchMetaOnly(projectOnlyOutput.map(_.name)) &&
      searchFiltersOnlyContainPartitionPredicates
    ) {
      projectOnlyOutput
    } else {
      vectorSearchOutput
    }
  }

  private def searchFiltersOnlyContainPartitionPredicates: Boolean = {
    val predicates = convertSearchFilters()
    if (predicates.isEmpty) {
      true
    } else {
      splitPartitionPredicatesAndDataPredicates(
        predicates.asJava,
        innerTable.rowType(),
        innerTable.partitionKeys()).getRight.isEmpty
    }
  }

  private def createSearchContext(
      rightProjection: UnsafeProjection,
      readerTracker: LateralVectorSearchReaderTracker,
      pinnedSnapshot: Option[Snapshot] = None,
      resultVectorSearchOutput: Seq[Attribute] = vectorSearchOutput): LateralVectorSearchContext = {
    val rowType = innerTable.rowType()
    val readFieldNames = resultVectorSearchOutput
      .filterNot(
        attr =>
          attr.name == PaimonMetadataColumn.SEARCH_SCORE_COLUMN ||
            attr.name == SpecialFields.ROW_ID.name())
      .map(_.name)
    val readFieldNamesWithRowId = readFieldNames :+ SpecialFields.ROW_ID.name()
    val rowTypeWithRowId = SpecialFields.rowTypeWithRowId(rowType)
    val readRowType = rowType.project(readFieldNames.asJava)
    val readRowTypeWithRowId = SpecialFields.rowTypeWithRowId(readRowType)
    val readBuilder = innerTable
      .newReadBuilder()
      .withReadType(rowTypeWithRowId.project(readFieldNamesWithRowId.asJava))
    val physicalReadBuilder = innerTable
      .newReadBuilder()
      .withReadType(readRowType)
    val scoreMetadataColumns =
      if (resultVectorSearchOutput.exists(_.name == PaimonMetadataColumn.SEARCH_SCORE_COLUMN)) {
        Seq(PaimonMetadataColumn.SEARCH_SCORE)
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
      .newBatchVectorSearchBuilder()
      .withVectorColumn(columnName)
      .withLimit(limit)
      .withOptions(options.asJava)
    val routingVectorSearchBuilder = innerTable
      .newBatchVectorSearchBuilder()
      .withVectorColumn(columnName)
      .withLimit(limit)
      .withOptions(LateralVectorSearchExec.partitionBasedRoutingOptions(options).asJava)
    pinnedSnapshot.foreach {
      snapshot =>
        readBuilder.withSnapshot(snapshot)
        physicalReadBuilder.withSnapshot(snapshot)
        vectorSearchBuilder.withSnapshot(snapshot)
        routingVectorSearchBuilder.withSnapshot(snapshot)
    }
    pushSearchFilters(
      Seq(readBuilder, physicalReadBuilder),
      Seq(vectorSearchBuilder, routingVectorSearchBuilder))

    val vectorPlan = if (planVectorSearchPerBatch) {
      None
    } else {
      Some(vectorSearchBuilder.newVectorScan().scan())
    }
    val batchSize =
      Math.max(1, new CoreOptions(innerTable.options()).vectorSearchLateralJoinBatchSize())

    LateralVectorSearchContext(
      readBuilder,
      physicalReadBuilder,
      vectorSearchBuilder,
      routingVectorSearchBuilder,
      vectorPlan,
      scoreMetadataColumns,
      sparkRow,
      rowIdOrdinal = resultRowType.getFieldIndex(SpecialFields.ROW_ID.name()),
      metaColumnsOnly =
        VectorSearchResultUtils.isVectorSearchMetaOnly(resultVectorSearchOutput.map(_.name)),
      projectionInputOrdinals = resultVectorSearchOutput.map {
        attr =>
          if (attr.name == PaimonMetadataColumn.SEARCH_SCORE_COLUMN) {
            -1
          } else {
            resultRowType.getFieldIndex(attr.name)
          }
      },
      rightProjection,
      resultVectorSearchOutput,
      batchSize,
      readerTracker
    )
  }

  private def attributesSchema(attributes: Seq[Attribute]): String = {
    attributes.map(attr => s"${attr.name}: ${attr.dataType.simpleString}").mkString("[", ", ", "]")
  }

  private def pushSearchFilters(
      readBuilders: Seq[ReadBuilder],
      vectorSearchBuilders: Seq[BatchVectorSearchBuilder]): Unit = {
    val predicates = convertSearchFilters()
    if (predicates.nonEmpty) {
      val split = splitPartitionPredicatesAndDataPredicates(
        predicates.asJava,
        innerTable.rowType(),
        innerTable.partitionKeys())
      if (split.getLeft.isPresent) {
        val partitionFilter = split.getLeft.get()
        readBuilders.foreach(_.withPartitionFilter(partitionFilter))
        vectorSearchBuilders.foreach(_.withPartitionFilter(partitionFilter))
      }
      if (!split.getRight.isEmpty) {
        val dataFilter = PredicateBuilder.and(split.getRight)
        readBuilders.foreach(_.withFilter(dataFilter))
        vectorSearchBuilders.foreach(_.withFilter(dataFilter))
      }
    }
  }

  private def convertSearchFilters(): Seq[Predicate] = {
    if (searchFilters.isEmpty) {
      Seq.empty
    } else {
      PaimonTableValuedFunctions
        .convertLateralVectorSearchFilters(
          innerTable,
          vectorSearchOutput,
          projectList,
          projectOutput,
          searchFilters)
        .getOrElse {
          throw new UnsupportedOperationException(
            s"Cannot convert searched-table predicates for LATERAL vector_search: $searchFilters")
        }
    }
  }

  private def search(
      queries: Seq[LateralVectorSearchQuery],
      context: LateralVectorSearchContext): Iterator[(InternalRow, InternalRow)] = {
    val vectors = queries.map(_.queryVector).toArray
    val globalIndexResults = context.vectorSearchBuilder
      .withVectors(vectors)
      .newBatchVectorRead()
      .readBatch(vectorPlan(context))
      .asScala
      .toVector
    // Batch vector search must return one result per input query vector and preserve the input
    // order, because createRowIdToMatches pairs each result with its original outer row by index.
    require(
      globalIndexResults.size == queries.size,
      s"Batch vector search returned ${globalIndexResults.size} results for ${queries.size} " +
        "query vectors. The result count must match the query count."
    )
    val primaryKeyResults = primaryKeyVectorResults(globalIndexResults)
    if (context.metaColumnsOnly) {
      return primaryKeyResults match {
        case Some(results) => searchPrimaryKeyMetaColumns(queries, results, context)
        case None => searchMetaColumns(queries, globalIndexResults, context)
      }
    }
    primaryKeyResults.foreach(results => return searchPrimaryKeyRows(queries, results, context))
    val rowIdToMatches = createRowIdToMatches(queries, globalIndexResults)
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
        val readerState = context.readerTracker.track(reader)
        new Iterator[Iterator[(InternalRow, InternalRow)]] {
          override def hasNext: Boolean = {
            val hasNext = reader.hasNext
            if (!hasNext) {
              readerState.closeOnce()
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

  private def searchPrimaryKeyMetaColumns(
      queries: Seq[LateralVectorSearchQuery],
      results: Seq[PrimaryKeyVectorResult],
      context: LateralVectorSearchContext): Iterator[(InternalRow, InternalRow)] = {
    queries.zip(results).iterator.flatMap {
      case (query, result) =>
        result.positions().iterator().asScala.map {
          position =>
            val values = context.resultVectorSearchOutput.map {
              attr =>
                attr.name match {
                  case PaimonMetadataColumn.ROW_ID_COLUMN => position.rowPosition()
                  case PaimonMetadataColumn.SEARCH_SCORE_COLUMN => position.score()
                  case name =>
                    throw new IllegalArgumentException(
                      s"Unsupported primary-key vector search metadata column: $name")
                }
            }.toArray
            val projectedRow = context.rightProjection(
              new JoinedRow(
                query.outerRow,
                new GenericInternalRow(values.asInstanceOf[Array[Any]])))
            (query.outerRow, projectedRow)
        }
    }
  }

  private def primaryKeyVectorResults(
      globalIndexResults: Seq[GlobalIndexResult]): Option[Seq[PrimaryKeyVectorResult]] = {
    val results = globalIndexResults.collect { case result: PrimaryKeyVectorResult => result }
    require(
      results.isEmpty || results.size == globalIndexResults.size,
      "Batch vector search cannot mix primary-key physical results with global row-ID results."
    )
    if (results.isEmpty) None else Some(results)
  }

  private def searchPrimaryKeyRows(
      queries: Seq[LateralVectorSearchQuery],
      results: Seq[PrimaryKeyVectorResult],
      context: LateralVectorSearchContext): Iterator[(InternalRow, InternalRow)] = {
    val snapshotId = results.head.snapshotId()
    require(
      results.forall(_.snapshotId() == snapshotId),
      "Primary-key batch vector results must belong to the same snapshot."
    )

    val positionToMatches = scala.collection.mutable
      .LinkedHashMap[LateralVectorSearchPhysicalPosition, ArrayBuffer[LateralVectorSearchMatch]]()
    val uniquePositions = scala.collection.mutable
      .LinkedHashMap[LateralVectorSearchPhysicalPosition, PrimaryKeySearchPosition]()
    queries.zip(results).foreach {
      case (query, result) =>
        result.positions().asScala.foreach {
          position =>
            val key = LateralVectorSearchPhysicalPosition.from(position)
            positionToMatches.getOrElseUpdate(key, ArrayBuffer()) +=
              LateralVectorSearchMatch(query.outerRow, position.score())
            uniquePositions.getOrElseUpdate(key, position)
        }
    }

    val sourceSplits =
      scala.collection.mutable.LinkedHashMap[LateralVectorSearchPhysicalFile, DataSplit]()
    results.foreach {
      result =>
        result.splits().asScala.foreach {
          split =>
            val dataSplit = split.dataSplit()
            val key = LateralVectorSearchPhysicalFile.from(dataSplit)
            sourceSplits.getOrElseUpdate(key, dataSplit)
        }
    }
    val batchResult = new PrimaryKeyScoredResult(
      snapshotId,
      sourceSplits.values.toList.asJava,
      uniquePositions.values.toList.asJava)
    val scan = context.physicalReadBuilder
      .newScan()
      .withGlobalIndexResult(batchResult)
      .asInstanceOf[InnerTableScan]
    val read = context.physicalReadBuilder.newRead()

    scan.plan().splits().asScala.iterator.flatMap {
      split =>
        val indexedSplit = split.asInstanceOf[IndexedSplit]
        val dataSplit = indexedSplit.dataSplit()
        val file = LateralVectorSearchPhysicalFile.from(dataSplit)
        val reader =
          PaimonRecordReaderIterator(
            read.createReader(split),
            Seq(PaimonMetadataColumn.ROW_ID) ++ context.scoreMetadataColumns,
            split)
        val readerState = context.readerTracker.track(reader)
        new Iterator[Iterator[(InternalRow, InternalRow)]] {
          override def hasNext: Boolean = {
            val hasNext = reader.hasNext
            if (!hasNext) {
              readerState.closeOnce()
            }
            hasNext
          }

          override def next(): Iterator[(InternalRow, InternalRow)] = {
            val rightRow = context.sparkRow.replace(reader.next())
            val position = LateralVectorSearchPhysicalPosition(
              file.partition,
              file.bucket,
              file.dataFileName,
              rightRow.getLong(context.rowIdOrdinal))
            positionToMatches.getOrElse(position, Seq.empty).iterator.map {
              searchMatch =>
                val projectedRow = projectRightRow(rightRow, searchMatch, context)
                (searchMatch.outerRow, projectedRow)
            }
          }
        }.flatMap(identity)
    }
  }

  private def searchMetaColumns(
      queries: Seq[LateralVectorSearchQuery],
      globalIndexResults: Seq[GlobalIndexResult],
      context: LateralVectorSearchContext): Iterator[(InternalRow, InternalRow)] = {
    queries.zip(globalIndexResults).iterator.flatMap {
      case (query, result) =>
        val scoreGetter = result match {
          case scored: ScoredGlobalIndexResult => Some(scored.scoreGetter())
          case _ => None
        }
        result.results().iterator().asScala.map {
          rowId =>
            val values = context.resultVectorSearchOutput
              .map(attr => VectorSearchResultUtils.valueOf(attr.name, rowId, scoreGetter))
              .toArray
            val projectedRow = context.rightProjection(
              new JoinedRow(
                query.outerRow,
                new GenericInternalRow(values.asInstanceOf[Array[Any]])))
            (query.outerRow, projectedRow)
        }
    }
  }

  private def projectRightRow(
      rightRow: InternalRow,
      searchMatch: LateralVectorSearchMatch,
      context: LateralVectorSearchContext): InternalRow = {
    val values = new Array[Any](context.resultVectorSearchOutput.size)
    context.resultVectorSearchOutput.zipWithIndex.foreach {
      case (attr, index) =>
        val ordinal = context.projectionInputOrdinals(index)
        values(index) = if (ordinal < 0) {
          searchMatch.score
        } else {
          rightRow.get(ordinal, attr.dataType)
        }
    }
    context.rightProjection(new JoinedRow(searchMatch.outerRow, new GenericInternalRow(values)))
  }

  private def projectPartitionBasedMetaOnly(
      matches: Seq[(InternalRow, PartitionBasedMatch)],
      context: LateralVectorSearchContext): Iterator[(InternalRow, InternalRow)] = {
    matches.iterator.map {
      case (outerRow, matchResult) =>
        val values = context.resultVectorSearchOutput
          .map(
            attr =>
              attr.name match {
                case PaimonMetadataColumn.ROW_ID_COLUMN => matchResult.rowId
                case PaimonMetadataColumn.SEARCH_SCORE_COLUMN => matchResult.score
                case name =>
                  throw new IllegalArgumentException(
                    s"Unsupported partition-based vector search metadata column: $name")
              })
          .toArray
        val projectedRow = context.rightProjection(
          new JoinedRow(outerRow, new GenericInternalRow(values.asInstanceOf[Array[Any]])))
        (outerRow, projectedRow)
    }
  }

  private def fetchPartitionBasedRows(
      matches: Seq[(InternalRow, PartitionBasedMatch)],
      context: LateralVectorSearchContext): Iterator[(InternalRow, InternalRow)] = {
    val rowIdToMatches = matches
      .groupBy(_._2.rowId)
      .mapValues(_.map {
        case (outerRow, matchResult) =>
          LateralVectorSearchMatch(outerRow, matchResult.score)
      })
    val rowIds = new RoaringNavigableMap64()
    rowIdToMatches.keys.foreach(rowIds.add)
    val scan = context.readBuilder
      .newScan()
      .withGlobalIndexResult(GlobalIndexResult.create(rowIds))
      .asInstanceOf[InnerTableScan]
    val read = context.readBuilder.newRead()

    scan.plan().splits().asScala.iterator.flatMap {
      split =>
        val reader =
          PaimonRecordReaderIterator(read.createReader(split), context.scoreMetadataColumns, split)
        val readerState = context.readerTracker.track(reader)
        new Iterator[Iterator[(InternalRow, InternalRow)]] {
          override def hasNext: Boolean = {
            val hasNext = reader.hasNext
            if (!hasNext) {
              readerState.closeOnce()
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
                query.outerRow,
                scoreGetter.map(_.score(rowId)).getOrElse(Float.NaN))
        }
    }
    rowIdToMatches.iterator.map { case (rowId, matches) => rowId -> matches.toSeq }.toMap
  }

  private def createBatchGlobalIndexResult(
      globalIndexResults: Seq[GlobalIndexResult]): GlobalIndexResult = {
    val rowIds = new RoaringNavigableMap64()
    globalIndexResults.foreach(result => rowIds.or(result.results()))
    GlobalIndexResult.create(rowIds)
  }

  private def toFloatArray(value: Any): Option[Array[Float]] = {
    value match {
      case null => None
      case arrayData: ArrayData => Some(arrayData.toFloatArray())
      case _ =>
        throw new RuntimeException(s"Cannot extract query vector from expression value: $value")
    }
  }

  private def vectorPlan(context: LateralVectorSearchContext): VectorScan.Plan = {
    context.vectorPlan.getOrElse(context.vectorSearchBuilder.newVectorScan().scan())
  }

  private class LateralVectorSearchReaderTracker {
    @volatile private var currentReader: LateralVectorSearchReaderState = _

    def track(reader: PaimonRecordReaderIterator): LateralVectorSearchReaderState = {
      val state = new LateralVectorSearchReaderState(reader, this)
      this.synchronized {
        currentReader = state
      }
      state
    }

    def clear(state: LateralVectorSearchReaderState): Unit = {
      this.synchronized {
        if (currentReader eq state) {
          currentReader = null
        }
      }
    }

    def closeCurrent(): Unit = {
      val reader = currentReader
      if (reader != null) {
        reader.closeOnce()
      }
    }
  }

  private class LateralVectorSearchReaderState(
      reader: PaimonRecordReaderIterator,
      tracker: LateralVectorSearchReaderTracker) {
    private var closed = false

    def closeOnce(): Unit = {
      this.synchronized {
        if (!closed) {
          closed = true
          try {
            reader.close()
          } finally {
            tracker.clear(this)
          }
        }
      }
    }
  }

  private case class LateralVectorSearchContext(
      readBuilder: ReadBuilder,
      physicalReadBuilder: ReadBuilder,
      vectorSearchBuilder: BatchVectorSearchBuilder,
      routingVectorSearchBuilder: BatchVectorSearchBuilder,
      vectorPlan: Option[VectorScan.Plan],
      scoreMetadataColumns: Seq[PaimonMetadataColumn],
      sparkRow: SparkInternalRow,
      rowIdOrdinal: Int,
      metaColumnsOnly: Boolean,
      projectionInputOrdinals: Seq[Int],
      rightProjection: UnsafeProjection,
      resultVectorSearchOutput: Seq[Attribute],
      batchSize: Int,
      readerTracker: LateralVectorSearchReaderTracker)

  private case class LateralVectorSearchQuery(outerRow: InternalRow, queryVector: Array[Float])

  private case class LateralVectorSearchMatch(outerRow: InternalRow, score: Float)

  private def sortPartitionBasedMatches(
      matches: Seq[PartitionBasedMatch]): Seq[PartitionBasedMatch] = {
    matches.sortBy(matchResult => (-matchResult.score, matchResult.rowId))
  }

  private def trimPartitionBasedMatches(
      matches: ArrayBuffer[PartitionBasedMatch]): ArrayBuffer[PartitionBasedMatch] = {
    if (matches.size > limit) {
      val topK = sortPartitionBasedMatches(matches).take(limit)
      matches.clear()
      matches ++= topK
    }
    matches
  }

  private case class LateralVectorSearchPhysicalFile(
      partition: BinaryRow,
      bucket: Int,
      dataFileName: String)

  private object LateralVectorSearchPhysicalFile {
    def from(split: DataSplit): LateralVectorSearchPhysicalFile = {
      require(
        split.dataFiles().size() == 1,
        "Primary-key indexed split must contain exactly one data file."
      )
      LateralVectorSearchPhysicalFile(
        split.partition().copy(),
        split.bucket(),
        split.dataFiles().get(0).fileName())
    }
  }

  private case class LateralVectorSearchPhysicalPosition(
      partition: BinaryRow,
      bucket: Int,
      dataFileName: String,
      rowPosition: Long)

  private object LateralVectorSearchPhysicalPosition {
    def from(position: PrimaryKeySearchPosition): LateralVectorSearchPhysicalPosition = {
      LateralVectorSearchPhysicalPosition(
        position.partition().copy(),
        position.bucket(),
        position.dataFileName(),
        position.rowPosition())
    }
  }
}

/** Spark physical node for the partition-based lateral vector search mode. */
class PartitionBasedLateralVectorSearchExec(
    override val innerTable: InnerTable,
    override val columnName: String,
    override val queryVectorExpr: Expression,
    override val limit: Int,
    override val options: Map[String, String],
    override val vectorSearchOutput: Seq[Attribute],
    override val projectList: Seq[Expression],
    override val projectOutput: Seq[Attribute],
    override val searchFilters: Seq[Expression],
    override val child: SparkPlan,
    override val planVectorSearchPerBatch: Boolean = true)
  extends LateralVectorSearchExec(
    innerTable,
    columnName,
    queryVectorExpr,
    limit,
    options,
    vectorSearchOutput,
    projectList,
    projectOutput,
    searchFilters,
    child,
    planVectorSearchPerBatch) {

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    new PartitionBasedLateralVectorSearchExec(
      innerTable,
      columnName,
      queryVectorExpr,
      limit,
      options,
      vectorSearchOutput,
      projectList,
      projectOutput,
      searchFilters,
      newChildren.head,
      planVectorSearchPerBatch
    )
  }
}
