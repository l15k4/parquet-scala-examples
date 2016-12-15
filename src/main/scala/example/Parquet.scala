package example

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.compat.FilterCompat.Filter
import org.apache.parquet.hadoop.example.{GroupReadSupport, GroupWriteSupport}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}
import org.apache.parquet.schema.MessageType

import scala.collection.mutable.ListBuffer

case class Record(cid: String, diid: String, gwid: String, dvc: String, geo: String, timestamp: Long, kvs: Map[String, String])

object Parquet {

  def read(parquetFile: Path, configuration: Configuration, filter: Filter = FilterCompat.NOOP): Seq[Record] = {
    val reader = ParquetReader.builder(new GroupReadSupport, parquetFile).withConf(configuration).withFilter(filter).build
    try {
      Iterator.continually(reader.read()).takeWhile(_ != null).foldLeft(ListBuffer.empty[Record]) {
        case (acc, group) =>
          val kvsGroup = group.getGroup("kvs", 0)
          val kvs =
            (0 until kvsGroup.getFieldRepetitionCount("kv")).foldLeft(Map.empty[String,String]) { case (result, idx) =>
              val kvGroup = kvsGroup.getGroup("kv", idx)
              result updated (kvGroup.getString("key", 0), kvGroup.getString("value", 0))
            }

          acc +=
            Record(
              group.getString("cid", 0),
              group.getString("diid", 0),
              group.getString("gwid", 0),
              group.getString("dvc", 0),
              group.getString("geo", 0),
              group.getLong("timestamp", 0),
              kvs
            )
      }
    } finally reader.close()
  }

  def writeHadoop(records: Seq[Record], schema: MessageType, outFile: Path, configuration: Configuration, blockSize: Int, pageSize: Int, dictPageSize: Int, codec: CompressionCodecName) {
    require(!outFile.getFileSystem(configuration).exists(outFile))

    configuration.set("parquet.example.schema", schema.toString)

    val writer = new ParquetWriter[Group](outFile, new GroupWriteSupport, codec, blockSize, pageSize, dictPageSize, true, true, ParquetProperties.WriterVersion.PARQUET_2_0, configuration)

    try {
      records.foreach { case Record(cid, diid, gwid, dvc, geo, timestamp, kvs) =>

        val group = new SimpleGroup(schema)
          .append("timestamp", timestamp)
          .append("cid", cid)
          .append("diid", diid)
          .append("gwid", gwid)
          .append("dvc", dvc)
          .append("geo", geo)

        val kvsGroup = group.addGroup("kvs")
        kvs.foreach { case (k,v) =>
          kvsGroup
            .addGroup("kv")
            .append("key",k)
            .append("value",v)
        }
        writer.write(group)
      }
    } finally writer.close()
  }
}
