package com.microsoft.sparkstreaming.eventhub

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{ReceiverInputDStream, DStream}
import org.apache.spark.Logging

import java.net.URLEncoder

import com.microsoft.eventhubs.client.IEventHubFilter

/**
 * @author jizuo
 */

object EventHubUtils extends Logging {
  private var domainSuffix: String = null
  private var sbNamespace: String = null
  private var sasPolicy: String = null
  private var sasKey: String = null
  
  private var storageAccountName: String = null
  private var storageAccountKey: String = null
  
  def config(domainSuffix: String, sbNamespace: String, sasPolicy: String, 
      sasKey: String, storageAccountName: String, storageAccountKey: String) {
    this.domainSuffix = domainSuffix
    this.sbNamespace = sbNamespace
    this.sasPolicy = sasPolicy
    this.sasKey = sasKey
    this.storageAccountName = storageAccountName
    this.storageAccountKey = storageAccountKey
    
    
  }
  
  def createStream(
    ssc: StreamingContext,
    eventHubName: String,
    partitionId: String,
    consumerGroupName: String,
    defaultCredits: Int = 1024,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
  ): ReceiverInputDStream[String] = {
    
    new EventHubInputDStream(ssc, getConnectionString(), eventHubName, partitionId, 
        consumerGroupName, defaultCredits, getStorageEndpointSuffix(), storageAccountName, 
        storageAccountKey, storageLevel)
  }
  
  def createStream(
      ssc: StreamingContext,
      eventHubName: String,
      numPartitions: Int,
      consumerGroupName: String
    ): DStream[String] = {
    val streams = (0 to numPartitions-1).map(i => createStream(ssc, eventHubName, i.toString, consumerGroupName))
    ssc.union(streams)
  }
  
  private def getConnectionString(): String = {
    var connectionStr: String = null
    
    try {
      val encodedKey: String = URLEncoder.encode(sasKey, "UTF-8")
      connectionStr = "amqps://" + sasPolicy + ":" + encodedKey + "@" + sbNamespace + ".servicebus." + domainSuffix
    } catch {
      case e: Exception => logError("Failed to encode sasKey of event hub: ${e.getCause().getMessage()}")
    }
    connectionStr
  }
  
  private def getStorageEndpointSuffix(): String = {
    var endpointSuffix: String = null
    
    endpointSuffix = "blob.core." + domainSuffix
    endpointSuffix
  }
}