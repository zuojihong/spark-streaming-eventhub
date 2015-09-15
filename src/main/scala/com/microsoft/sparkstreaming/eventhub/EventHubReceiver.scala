package com.microsoft.sparkstreaming.eventhub

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver

import com.microsoft.eventhubs.client._

/**
 * @author jizuo
 */
class EventHubReceiver(
    connectionString: String,
    eventHubName: String,
    partitionId: String,
    consumerGroupName: String,
    defaultCredits: Int,
    storageEndpointSuffix: String,
    storageAccountName: String,
    storageAccountKey: String,
    storageLevel: StorageLevel
  ) extends Receiver[String](storageLevel) with Logging {
  
  private var receiverImpl: ResilientEventHubReceiver = null 
  
  def onStart() {
    new Thread(s"Receiver for ${connectionString}/${eventHubName}/${partitionId}") {
      override def run() {   
        Checkpointer.init(storageEndpointSuffix, storageAccountName, storageAccountKey, 
            eventHubName, consumerGroupName, partitionId)
        
        receiverImpl = new ResilientEventHubReceiver(connectionString, eventHubName, partitionId, 
          consumerGroupName, defaultCredits, getOffsetFilter())
        
        try {
          receiverImpl.initialize()
        } catch {
          case e: EventHubException => restart("error connecting to event hub")
        }
        
        receive() 
      }
    }.start()
    
    
    logInfo(s"Receiver for ${connectionString}/${eventHubName}/${partitionId} initialized");
  }
  
  def onStop() {
    if (receiverImpl != null) {
      receiverImpl.close()
      receiverImpl = null
      logInfo(s"Receiver for ${connectionString}/${eventHubName}/${partitionId} stopped")
    }
  }
  
  private def getOffsetFilter(): IEventHubFilter = {
    val offset: String = Checkpointer.getLatestOffset()
    var filter: IEventHubFilter = null
    if (offset != null) {
      filter = new EventHubOffsetFilter(offset)
    }
    null
  }
  
  private def receive() {
    while (receiverImpl != null) {
      logInfo(s"Try to receive message from ${connectionString}/${eventHubName}/${partitionId}")
      var message: EventHubMessage = EventHubMessage.parseAmqpMessage(receiverImpl.receive(5000))
      if (message != null) {
        logInfo(s"Got message from receiver ${connectionString}/${eventHubName}/${partitionId}:\n  ${message.getDataAsString}")
        store(message.getDataAsString)
        Checkpointer.updateOffset(message.getOffset)
      }
    }
    logInfo("Message receiving stopped as receive becomes empty")
  }
  
  
}