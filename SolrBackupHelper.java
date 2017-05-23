package com.solr.backup;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.InvalidKeyException;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;

import org.apache.solr.client.solrj.SolrQuery.SortClause;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CursorMarkParams;
import org.json.JSONException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.CryptoConfiguration;
import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.google.gson.Gson;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

public class SolrBackupHelper {

	/**
	 * Method to Query and Export File using SolrQuery and Exporting it to
	 * folder using fileOutputStream
	 * 
	 * @param fileNameWithExtrn
	 * @param zkDetail
	 * @param fetchSize
	 * @param collectionName
	 * @param uniquekey
	 * @param seperator
	 * @param queryArgs
	 * @throws SolrServerException
	 * @throws IOException
	 * @throws JSONException
	 */

	public void queryAndExportToFile(String fileNameWithExtrn, String zkDetail, int fetchSize, String collectionName,
			String uniquekey, String queryArgs) throws SolrServerException, IOException, JSONException {
		FileOutputStream fos = new FileOutputStream("./" + fileNameWithExtrn, false);
		OutputStreamWriter writer = new OutputStreamWriter(fos, "UTF-8");
		CloudSolrClient cloudSolrClientObject = new CloudSolrClient(zkDetail);
		try {
			SolrQuery queryForValue = (new SolrQuery(queryArgs)).setStart(0).setRows(fetchSize).setParam("wt", "json")
					.setParam("collection", collectionName).setSort(SortClause.asc(uniquekey));
			
			// Cursor to iterate through collection to read values
			String cursorMark = CursorMarkParams.CURSOR_MARK_START;
			boolean done = false;
			int count = 0;
			while (!done) {
				queryForValue.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
				QueryResponse responseForValue = cloudSolrClientObject.query(queryForValue);
				if (responseForValue != null) {
					String nextCursorMark = responseForValue.getNextCursorMark();
					// write values to File
					writeRecordsToFile(responseForValue, writer, fileNameWithExtrn);
					if (cursorMark.equals(nextCursorMark)) {
						done = true;
					}
					cursorMark = nextCursorMark;
					count++;
				}
				System.out.println("Cursor " + count);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception Query and Export " + e.getLocalizedMessage());
			System.out.println("Exception " + e.getMessage());
		}
 
	}

	/**
	 * method to write record to File
	 * 
	 * @param queryResponseObject
	 * @param writer
	 * @throws IOException
	 * @throws JSONException
	 */
	public void writeRecordsToFile(QueryResponse queryResponseObject, OutputStreamWriter writer,
			String fileNameWithExtrn) throws IOException, JSONException {
		SolrDocumentList docList = queryResponseObject.getResults();
		Gson gson = new Gson();
		Long numfound = docList.getNumFound();
		for (int i = 0; i < docList.size(); i++) {
			SolrDocument sdoc = docList.get(i);
			String json = gson.toJson(sdoc);
			writer.write(json);
			writer.write(System.lineSeparator());
			writer.flush();
		}
		try {
			int counterForLine = countLines(fileNameWithExtrn);
			if (counterForLine != 0 && numfound == counterForLine) {
				System.out.println("Total no of records in solr:" + numfound);
				System.out.println("All " + numfound + " Records are successfully dump in File " + fileNameWithExtrn);
			}
		} catch (IOException ex) {
			ex.printStackTrace();
			System.out.println("Exception occur!You didn't able to fetch all records from Solr" + ex.getMessage());
		} catch (NullPointerException ex) {
			ex.printStackTrace();
			System.out.println("Exception occur!Zero data fetched" + ex.getMessage());
		}

	}

	/**
	 * method to count No of records are successfully stored by checking line no
	 * as each record is in one line
	 * 
	 * @param filename
	 */
	public int countLines(String filename) throws IOException {

		LineNumberReader reader = new LineNumberReader(new FileReader(filename));
		int count = 0;
		String lineRead = "";
		while ((lineRead = reader.readLine()) != null) {
		}
		count = reader.getLineNumber();
		reader.close();
		return count;
	}

	/**
	 * Method for Compressing the Exported File using Gzip
	 * 
	 * @param sourceFilepath
	 * @param destCompressedFilepath
	 * @throws IOException
	 */
	public void compressGzipFile(String sourceFilepath, String destCompressedFilepath) throws IOException {
		byte[] buffer = new byte[1024];
		// change done by kartik
		FileInputStream fileInput = null;
		FileOutputStream fileOutputStream = null;
		GZIPOutputStream gzipOuputStream = null;

		try {
			fileInput = new FileInputStream(sourceFilepath);
			fileOutputStream = new FileOutputStream(destCompressedFilepath);
			gzipOuputStream = new GZIPOutputStream(fileOutputStream);
			int bytes_read;
			while ((bytes_read = fileInput.read(buffer)) >= 0) {
				gzipOuputStream.write(buffer, 0, bytes_read);
			}
			System.out.println("The file " + sourceFilepath + " was compressed successfully with Name: " + destCompressedFilepath);
		} catch (IOException ex) {
			System.out.println(" error in file compression " + sourceFilepath);
		} finally {
			try {
				gzipOuputStream.finish();
				gzipOuputStream.close();
				fileOutputStream.close();
				fileInput.close();

			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("exception in closing compression" + e.getMessage());
			}

		}
	}

	/**
	 * Encrypting the Exported file using Server side encryption Asymmetric
	 * Encryption key using Cmk.
	 * 
	 * @param bucketName
	 * @param objectKey
	 * @param kms_cmk_id
	 * @param srcCompressedFileName
	 * @param aws_region
	 * @param src_file_loc
	 * @throws IOException
	 */

	public void encryptSSEExportedFile(String bucketName, String objectKey, String myAccessKeyId, String mySecretKey,
			File srcCompressedFileName,  Integer port, String proxy, String objectKeyName)
			throws IOException {
		AmazonS3 s3Obj;
		PutObjectRequest putRequest1 =null;
		
			if (("NA".equalsIgnoreCase(mySecretKey)) && ("NA".equalsIgnoreCase(myAccessKeyId))) {
				System.out.println("Creating InstanceProfileCredentialsProvider");
				//s3Obj = AmazonS3ClientBuilder.standard().withCredentials(new InstanceProfileCredentialsProvider()).build()
				s3Obj = AmazonS3ClientBuilder.standard().withCredentials(InstanceProfileCredentialsProvider.getInstance()).build();
			} else {
				BasicAWSCredentials awsCreds = new BasicAWSCredentials(myAccessKeyId, mySecretKey);
				ClientConfiguration cc = new ClientConfiguration();
				if (((proxy == null) && (port == 0)) || ((proxy.length() == 0) && (port == 0))) {
					s3Obj = new AmazonS3Client(awsCreds);

				} else {
					cc.setProxyHost(proxy);
					cc.setProxyPort(port);
					s3Obj = new AmazonS3Client(awsCreds, cc);
				}
			}
			 putRequest1 = new PutObjectRequest(bucketName, objectKey + srcCompressedFileName,
					srcCompressedFileName);
		
		// Request server-side encryption.
		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		putRequest1.setMetadata(objectMetadata);

		try {
			// Adding rule for expiration of object in 7 days
			/*BucketLifecycleConfiguration.Rule rule1 = new BucketLifecycleConfiguration.Rule()
					.withId("Archive immediately rule").withExpirationInDays(expirationInDays).withPrefix(objectKeyName)
					.withStatus(BucketLifecycleConfiguration.ENABLED.toString());
			BucketLifecycleConfiguration configuration = new BucketLifecycleConfiguration().withRules(rule1);
			// Save configuration.
			s3Obj.setBucketLifecycleConfiguration(bucketName, configuration);*/
			System.out.println("Uploading file: " + srcCompressedFileName + " in key " + objectKey + " to S3 bucket "
					+ bucketName);
			PutObjectResult response1 = s3Obj.putObject(putRequest1);
			System.out.println("Uploaded object encryption status is " + response1.getSSEAlgorithm());

		} catch (AmazonClientException ace) {
			ace.printStackTrace();
			System.out.println("Exception"+ace);
			System.out.println(
					"Caught an AmazonClientException, which means the client encountered an internal error while trying to communicate with S3");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}
	
	/**
	 * 
	 * 
	 * @param bucketName
	 * @param objectKey
	 * @param myAccessKeyId
	 * @param mySecretKey
	 * @param srcCompressedFileName
	 * @param expirationInDays
	 * @param port
	 * @param proxy
	 * @param objectKeyName
	 * @throws IOException
	 * @throws URISyntaxException 
	 * @throws InvalidKeyException 
	 * @throws StorageException 
	 */
	public void compressedAzureExportedFile(String bucketName,String objectKey,String storageConnectionString,File srcCompressedFileName)
			throws IOException, InvalidKeyException, URISyntaxException, StorageException {
		

		try {
			CloudStorageAccount account = CloudStorageAccount.parse(storageConnectionString);
			CloudBlobClient serviceClient = account.createCloudBlobClient();

			// Container name must be lower case.Create if not exist
			CloudBlobContainer container = serviceClient.getContainerReference(objectKey);
			// container.createIfNotExists();

			// Upload an image file.
			CloudBlockBlob blob = container.getBlockBlobReference((srcCompressedFileName.getName()));
			//File sourceFile = new File(srcCompressedFileName);
			blob.upload(new FileInputStream(srcCompressedFileName), srcCompressedFileName.length());
			
		} catch (FileNotFoundException fileNotFoundException) {
			fileNotFoundException.printStackTrace();
			System.out.print("FileNotFoundException encountered: ");
			System.out.println(fileNotFoundException.getMessage());
			System.exit(-1);
		} catch (StorageException storageException) {
			storageException.printStackTrace();
			System.out.print("StorageException encountered: ");
			System.out.println(storageException.getMessage());
			System.exit(-1);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.print("Exception encountered: ");
			System.out.println(e.getMessage());
			System.exit(-1);
		}
	}

	/**
	 * method for deleting compressed file in current location
	 * 
	 * @param destCompressedFilepath
	 */

	public void delete(final String destCompressedFile,String cloudStorage) {
		final File sourceFile = new File(destCompressedFile);
		sourceFile.delete();
		System.out.println("Your compressedFile " + destCompressedFile
				+ " has been uploaded to "+ cloudStorage +" and deleted from local file system");
	}

	/**
	 * Encrypting the Exported file using Client side encryption Asymmetric
	 * Encryption key using Cmk.
	 * 
	 * @param bucketName
	 * @param objectKey
	 * @param kms_cmk_id
	 * @param srcCompressedFileName
	 *            TODO
	 * @param aws_region
	 * @param src_file_loc
	 * @throws IOException
	 */
	public void encryptExportedFile(String bucketName, String objectKey, String kms_cmk_id, String myAccessKeyId,
			String mySecretKey, File srcCompressedFileName, Regions aws_region, int expirationInDays, int port,
			String proxy, String objectKeyName) throws IOException {

		// Construct an instance of AmazonS3EncryptionClient
		AmazonS3EncryptionClient encryptionClient;

		// BasicAWSCredentials propertiesCredentials;
		AWSCredentials credentials = new BasicAWSCredentials(myAccessKeyId, mySecretKey);

		// KmsEncryptionClientProvider class to provide Customerkey
		KMSEncryptionMaterialsProvider materialProvider = new KMSEncryptionMaterialsProvider(kms_cmk_id);

		// Clientconfiguration to connect through proxy
		ClientConfiguration cc = new ClientConfiguration();
		cc.setProxyHost(proxy);
		cc.setProxyPort(port);

		// creating a new S3EncryptionClient
		encryptionClient = new AmazonS3EncryptionClient(credentials, materialProvider, cc, new CryptoConfiguration());

		try {
			// Adding rule for expiration of object in 7 days
			/*
			 * BucketLifecycleConfiguration.Rule rule1 = new
			 * BucketLifecycleConfiguration.Rule() .withId(
			 * "Archive immediately rule"
			 * ).withExpirationInDays(expirationInDays).withPrefix(
			 * objectKeyName)
			 * .withStatus(BucketLifecycleConfiguration.ENABLED.toString());
			 * BucketLifecycleConfiguration configuration = new
			 * BucketLifecycleConfiguration().withRules(rule1); // Save
			 * configuration.
			 * encryptionClient.setBucketLifecycleConfiguration(bucketName,
			 * configuration);
			 */
			System.out.println("Uploading file: " + objectKey + " to S3 bucket " + bucketName);
			// s3.putObject(bucketName,objectKey,Src_compressed_File);
			encryptionClient.putObject(bucketName, objectKey, srcCompressedFileName);
			// encryptionClient.putObject(bucketName, objectKey, new
			// File(schemaFile));

		} catch (AmazonClientException ace) {
			ace.printStackTrace();
			System.out.println("Exception"+ace);
			System.out.println(
					"Caught an AmazonClientException, which means the client encountered an internal error while trying to communicate with S3");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	/**
	 * Compressed File Upload to S3 without Encryption
	 * 
	 * @param bucketName
	 * @param objectKey
	 * @param kms_cmk_id
	 * @param myAccessKeyId
	 * @param mySecretKey
	 * @param Src_compressed_File
	 * @param aws_region
	 * @param port
	 * @param proxy
	 * @throws IOException
	 */
	public void compressedFileUploadToS3(String bucketName, String objectKey, String kms_cmk_id, String myAccessKeyId,
			String mySecretKey, File Src_compressed_File, Regions aws_region, int port, String proxy)
			throws IOException {

		AmazonS3Client asc;
		ClientConfiguration cc = new ClientConfiguration();
		cc.setProxyHost(proxy);
		cc.setProxyPort(port);

		// Construct an instance of AmazonS3EncryptionClient
		AWSCredentials credentials = new BasicAWSCredentials(myAccessKeyId, mySecretKey);

		// creating a new S3EncryptionClient
		asc = new AmazonS3Client(credentials, cc);
		try {
			System.out.println("Uploading file: " + objectKey + " to S3 bucket " + bucketName);
			asc.putObject(bucketName, objectKey, Src_compressed_File);

		} catch (AmazonClientException ace) {
			ace.printStackTrace();
			System.out.println("Exception"+ace);
			System.out.println(
					"Caught an AmazonClientException, which means the client encountered an internal error while trying to communicate with S3");
			System.out.println("Error Message: " + ace.getMessage());
		}

	}

	/**
	 * 
	 * @param collectionName
	 * @param compressedFileName
	 * @throws Exception
	 */
	public void testBackup(String collectionName, String compressedFileName) throws Exception {
		String backupLocation = "./";
		String backupName = collectionName + "_backup";
		SolrClient client = new HttpSolrClient(
				"http://linux152.ad.infosys.com:8983/solr/RetailQA_Protocol_shard1_replica1/admin/collections?action=BACKUP&name="
						+ backupName + "&collection=" + collectionName + "&location=" + backupLocation);
		// /admin/collections?action=BACKUP&name=myBackupName&collection=myCollectionName&location=/path/to/my/shared/drive
		// String backupUrl=zkHost +"/" + collectionName + "?"
		// +"action=BACKUP&"+"name="+backupName+"&collection="+collectionName+"&location="+backupLocation;
		System.out.println("Backing Up");

	}

	/**
	 * 
	 * @param collectionName
	 * @param myRestoredCollectionName
	 * @param compressedFileName
	 * @throws Exception
	 */
	public void testRestore(String collectionName, String myRestoredCollectionName, String compressedFileName)
			throws Exception {
		String backupLocation = "./";
		String backupName = collectionName + "_backup";
		SolrClient client = new HttpSolrClient(
				"http://linux152.ad.infosys.com:8983/solr/RetailQA_Protocol_shard1_replica1/admin/collections?action=RESTORE&name="
						+ backupName + "&location=" + backupLocation + "&collection=" + myRestoredCollectionName);
		// /admin/collections?action=BACKUP&name=myBackupName&collection=myCollectionName&location=/path/to/my/shared/drive
		// String backupUrl=zkHost +"/" + collectionName + "?"
		// +"action=BACKUP&"+"name="+backupName+"&collection="+collectionName+"&location="+backupLocation;
		System.out.println("Restoring Up");

	}

	/**
	 * method to get configuration file you have to take variable to set file
	 * name
	 * 
	 * @param collectionNameWithShardReplica
	 * @param exportedSchemaFile
	 * @param configFile
	 * @throws IOException
	 */
	public void getConfigurationSchema(String collectionNameWithShardReplica, String exportedSchemaFile,
			String configFile) throws IOException {

		String url = "http://linux152.ad.infosys.com:8983/solr/RetailQA_Protocol_shard1_replica1/admin/file?file=schema.xml&contentType=text/xml;charset=utf-8";
		FileUtils.copyURLToFile(new URL(url), new File("./Schema.xml"));

	}

}
