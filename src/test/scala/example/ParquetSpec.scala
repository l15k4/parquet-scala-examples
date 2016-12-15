package example

import java.io.File
import java.util.UUID

import io.druid.data.input.parquet.DruidParquetInputFormat
import io.druid.indexer.HadoopDruidIndexerConfig
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{Job, TaskAttemptID}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.util.ReflectionUtils
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter, ParquetWriter}
import org.apache.parquet.schema.OriginalType._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.{OriginalType, Types}
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{BeforeAndAfterAll, FreeSpec}

import scala.util.Random

class ParquetSpec extends FreeSpec with BeforeAndAfterAll {

  private val targetFilePath ="target/parquet/base.parquet"
  private val targetFile = new Path(targetFilePath)

  private val geos = Array("abc", "def", "efh", "ghi", "jkl", "mno", "pqr", "stu", "vwx", "yz")
  private val dim = Array("123", "456", "789", "306", "683", "122", "045", "938", "446", "821")
  private val dvcs = Array("p", "c", "t", "pda", "tv", "gc", "-")

  private val start = new DateTime(2016, 1, 1, 1, 0, 0, DateTimeZone.UTC)

  private val conf = new Configuration()

  def testRecord(time: DateTime) =
    Record(
      "c0051",
      UUID.randomUUID().toString,
      UUID.randomUUID().toString,
      dvcs(Random.nextInt(dvcs.length)),
      geos(Random.nextInt(geos.length)),
      time.getMillis,
      Map(
        "d_a" -> dim(Random.nextInt(geos.length)),
        "d_b" -> dim(Random.nextInt(geos.length)),
        "d_c" -> dim(Random.nextInt(geos.length))
      )
    )

  override def beforeAll() =
    try super.beforeAll() finally targetFile.getFileSystem(conf).delete(targetFile.getParent, true)

  "test write and read" in {

    val expectedRecords = (1 to 10000).map(i => testRecord(start.plusMillis(i)))

    val schema = {
      Types.buildMessage().addFields(
        Types.required(PrimitiveTypeName.INT64).as(OriginalType.TIMESTAMP_MILLIS).named("timestamp"),
        Types.required(PrimitiveTypeName.BINARY).as(OriginalType.ENUM).named("cid"),
        Types.required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("gwid"),
        Types.required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("diid"),
        Types.required(PrimitiveTypeName.BINARY).as(OriginalType.ENUM).named("dvc"),
        Types.required(PrimitiveTypeName.BINARY).as(OriginalType.ENUM).named("geo"),
        Types.requiredGroup.as(MAP).repeatedGroup.as(MAP_KEY_VALUE)
          .required(BINARY).as(ENUM).named("key")
          .required(BINARY).as(ENUM).named("value")
          .named("kv").named("kvs")
      ).named("base")
    }

    Parquet.writeHadoop(
      expectedRecords,
      schema,
      targetFile,
      conf,
      ParquetWriter.DEFAULT_BLOCK_SIZE,
      ParquetWriter.DEFAULT_PAGE_SIZE,
      512,
      CompressionCodecName.GZIP
    )

    val outputStatus = targetFile.getFileSystem(conf).getFileStatus(targetFile.getParent)
    val footers = ParquetFileReader.readFooters(conf, outputStatus, false)
    println(footers.get(0))

    ParquetFileWriter.writeMetadataFile(conf, targetFile.getParent, footers)

    // note that any way of reading parquet files is extremely inefficient unless you use Spark-sql for example
    // that has a tooling for effective parquet file reading. For me this is the biggest disadvantage of Parquet
    val actualRecords = Parquet.read(targetFile, conf)
    assertResult(expectedRecords)(actualRecords)
  }

  "druid" in {
    val job = Job.getInstance(new Configuration())
    HadoopDruidIndexerConfig.fromFile(new File("src/test/resources/base_hadoop_parquet_job.json")).intoConfiguration(job)

    def getFirstRecord: GenericRecord = {
      val testFile = new File(targetFilePath)
      val path = new Path(testFile.getAbsoluteFile.toURI)
      val split = new FileSplit(path, 0, testFile.length(), null)

      val inputFormat = ReflectionUtils.newInstance(classOf[DruidParquetInputFormat], job.getConfiguration)
      val context = new TaskAttemptContextImpl(job.getConfiguration, new TaskAttemptID())
      val reader = inputFormat.createRecordReader(split, context)

      reader.initialize(split, context)
      reader.nextKeyValue()
      val data = reader.getCurrentValue
      reader.close()
      data
    }

    val record = getFirstRecord
    println(record)

    // Note that druid doesn't support parquet MAP, only flat group with primitive types :-/

    assertResult(null)(record.get("non-existing"))
    assert(record.get("gwid") != null)
    assert(record.get("diid") != null)
    assertResult(new Utf8("c0051"))(record.get("cid"))
    assert(geos.toSet[String].map(new Utf8(_)).contains(record.get("geo").asInstanceOf[Utf8]))
    assert(dvcs.toSet[String].map(new Utf8(_)).contains(record.get("dvc").asInstanceOf[Utf8]))

  }

}
