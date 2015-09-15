package com.microsoft.sparkstreaming.eventhub

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver

import com.microsoft.eventhubs.client.IEventHubFilter

/**
 * @author jizuo
 */
private[eventhub]
class EventHubInputDStream (
    ssc_ : StreamingContext,
    connectionString: String,
    eventHubName: String,
    partitionId: String,
    consumerGroupName: String,
    defaultCredits: Int,
    storageEndpointSuffix: String,
    storageAccountName: String,
    storageAccountKey: String,
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[String](ssc_) with Logging {
  
  def getReceiver() : Receiver[String] = {
    new EventHubReceiver(connectionString, eventHubName, partitionId, consumerGroupName, 
        defaultCredits, storageEndpointSuffix, storageAccountName, storageAccountKey, storageLevel)
  }
}