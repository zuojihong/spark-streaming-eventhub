package com.microsoft.sparkstreaming.eventhub

import com.microsoft.azure.storage.{CloudStorageAccount, StorageCredentialsAccountAndKey, StorageException}
import com.microsoft.azure.storage.blob.{BlobRequestOptions, CloudBlobClient, CloudBlobContainer, CloudBlockBlob, BlobOutputStream}

import java.net.{URI, URISyntaxException}

import org.apache.spark.Logging

/**
 * @author jizuo
 */
object AzureStorageUtils extends Logging {
  var blobClient: CloudBlobClient = null
  
  def getOrCreateContainer(endpointSuffix: String, accountName: String, accountKey: String, containerName: String): CloudBlobContainer = {
    var container: CloudBlobContainer = null
    try {
      val cred: StorageCredentialsAccountAndKey = new StorageCredentialsAccountAndKey(accountName, accountKey)
      val storageAccount = new CloudStorageAccount(cred, true, endpointSuffix)
      blobClient = storageAccount.createCloudBlobClient()
      container = blobClient.getContainerReference(containerName)
      container.createIfNotExists()
    } catch {
      case e: Exception => logError("Failed to create blob container: ${e.getCause().getMessage()}")
    }
    container
  }
  
  def writeBlockBlob(container: CloudBlobContainer, blobName: String, blobContent: String) {
    var outputStream: BlobOutputStream = null
    try {
      val blob = container.getBlockBlobReference(blobName)
      outputStream = blob.openOutputStream()
      outputStream.write(blobContent.getBytes("UTF-8"))    
    } catch {
      case e: Exception => logError("Failed to write to blob ${blobName} with content: ${e.getCause().getMessage()}")
    } 
    finally {
      outputStream.close()
    }
  }
  
  def readBlockBlob(container: CloudBlobContainer, blobName: String): String = {
    var content: String = null
    try {
      val blob = container.getBlockBlobReference(blobName)
      content = blob.downloadText("UTF-8", null, null, null)
      
    } catch {
      case e: Exception => logError("Failed to read blob ${blobName}: ${e.getCause().getMessage()}")
    }
    content
  }
}