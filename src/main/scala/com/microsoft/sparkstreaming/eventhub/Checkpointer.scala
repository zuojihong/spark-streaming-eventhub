package com.microsoft.sparkstreaming.eventhub

import com.microsoft.azure.storage.blob.CloudBlobContainer
import org.apache.spark.Logging
import com.fasterxml.jackson.core.{JsonGenerator, JsonFactory, JsonToken}
import java.io.ByteArrayOutputStream

/**
 * @author jizuo
 */
object Checkpointer extends Logging {
  private var container: CloudBlobContainer = null
  private var blobName: String = null
  private var partitionId: String = null
  private var latestOffset: String = "-1"
  private var lastCheckpointTime = System.currentTimeMillis()
  private val checkpointIntervalInMilli = 5 * 1000
  
  def init(storageEndpointSuffix: String, storageAccountName: String, storageAccountKey: String, 
      eventHubName: String, consumerGroupName: String, partitionId: String) {
    container = AzureStorageUtils.getOrCreateContainer(storageEndpointSuffix, storageAccountName, storageAccountKey, eventHubName)
    blobName = consumerGroupName + "/" + partitionId
    this.partitionId = partitionId
  }
  
  def getLatestOffset(): String = {
    var offset: String = null
    offset = parseOffsetJson(AzureStorageUtils.readBlockBlob(container, blobName))
    latestOffset = offset
    logInfo("Set latest offset to ${offset}")
    offset
  }
  
  def updateOffset(offset: String) {
    latestOffset = offset
    
    val curTime = System.currentTimeMillis()
    if (curTime - lastCheckpointTime >= checkpointIntervalInMilli) {
      commitOffset(latestOffset)
      lastCheckpointTime = System.currentTimeMillis()
    }
  }
  
  def commitOffset(offset: String) {
    AzureStorageUtils.writeBlockBlob(container, blobName, generateOffsetJson(offset))
  }
  
  private def generateOffsetJson(offset: String): String = {
    val outputStream = new ByteArrayOutputStream()
    val jsonFactory = new JsonFactory()
    val generator = jsonFactory.createGenerator(outputStream)
    
    generator.writeStartObject()
    generator.writeStringField("PartitionId", partitionId)
    generator.writeStringField("Offset", offset)
    generator.writeEndObject()
    generator.close()
    outputStream.toString("UTF-8")
  }
  
  private def parseOffsetJson(jsonStr: String): String = {
    var offset: String = null
    if (jsonStr != null) {
      val jsonFactory = new JsonFactory()
      val parser = jsonFactory.createParser(jsonStr)
      var token = parser.nextToken()
      
      try {
        while (! parser.isClosed() && token != null) {
          if (JsonToken.FIELD_NAME.equals(token) && "PartitionId".equals(parser.getCurrentName())) {
            token = parser.nextToken()
            val partId = parser.getText()
            if (!partitionId.equals(partId)) {
              throw new Exception("PartitionId in offset json (${partId}) not equal to that of checkpointer ${partitionId}")
            }
          }
          else if (JsonToken.FIELD_NAME.equals(token) && "Offset".equals(parser.getCurrentName())) {
            token = parser.nextToken()
            offset = parser.getText()
          }
          
          token = parser.nextToken()
        }
      } catch {
        case e: Exception => logError("Failed to parse offset string: ${jsonStr}\nReason: ${e.getCause().getMessage()}")
      }
    }
    else {
      logError("Invalid offset json string: ${jsonStr}")
    }
    offset
  }
}