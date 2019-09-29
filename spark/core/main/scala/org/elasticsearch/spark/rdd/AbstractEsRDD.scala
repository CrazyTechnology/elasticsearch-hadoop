package org.elasticsearch.spark.rdd;

import org.apache.commons.logging.LogFactory
import org.apache.spark.{Partition, SparkContext}
import org.apache.spark.rdd.RDD
import org.elasticsearch.hadoop.rest.{PartitionDefinition, RestRepository, RestService}
import org.elasticsearch.hadoop.util.ObjectUtils
import org.elasticsearch.spark.cfg.SparkSettingsManager

import scala.collection.JavaConversions.{collectionAsScalaIterable, mapAsJavaMap}
import scala.reflect.ClassTag

private[spark] abstract class AbstractEsRDD[T: ClassTag](
  @transient sc: SparkContext,
  val params: scala.collection.Map[String, String] = Map.empty)
  extends RDD[T](sc, Nil) {

  private val init = { ObjectUtils.loadClass("org.elasticsearch.spark.rdd.CompatUtils", classOf[ObjectUtils].getClassLoader) }

  @transient protected lazy val logger = LogFactory.getLog(this.getClass())

  override def getPartitions: Array[Partition] = {
    esPartitions.zipWithIndex.map { case(esPartition, idx) =>
      new EsPartition(id, idx, esPartition)
    }.toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val esSplit = split.asInstanceOf[EsPartition]
    esSplit.esPartition.getHostNames
  }

  override def checkpoint() {
    // Do nothing. Elasticsearch RDD should not be checkpointed.
  }

  def esCount(): Long = {
    val repo = new RestRepository(esCfg)
    try {
      repo.count(true)
    } finally {
      repo.close()
    }
  }

  @transient private[spark] lazy val esCfg = {
    val cfg = new SparkSettingsManager().load(sc.getConf).copy();
    cfg.merge(params)
  }

  //获取es分区信息
  @transient private[spark] lazy val esPartitions = {
    RestService.findPartitions(esCfg, logger)
  }
}

private[spark] class EsPartition(rddId: Int, idx: Int, val esPartition: PartitionDefinition)
  extends Partition {

  override def hashCode(): Int = 41 * (41 * (41 + rddId) + idx) + esPartition.hashCode()

  override val index: Int = idx
}