package com.solr.restore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FilenameUtils;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.json.simple.parser.ParseException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.model.CryptoConfiguration;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

import au.com.bytecode.opencsv.CSVReader;

/** 
 * 
 * @author Shalu_Mishra
 *
 */
public class SolrRecoveryHelper {

	private static JsonFactory factory;

	/**
	 * Method to  Download from AWS-S3
	 * @param myAccessKeyId
	 * @param mySecretKey
	 * @param bucketName
	 * @param objectKey
	 * @param proxy
	 * @param port
	 * @param objectKeyName
	 * @param compressedFilepath
	 * @throws IOException
	 */
	public void decryptSSEDownloadFromS3(String myAccessKeyId, String mySecretKey, String bucketName, String objectKey,
			String proxy, Integer port, String objectKeyName, String compressedFilepath) throws IOException {
		AmazonS3 s3Obj;
		if ( (("NA".equalsIgnoreCase(mySecretKey)) && ("NA".equalsIgnoreCase(myAccessKeyId))) ) {
			s3Obj = AmazonS3ClientBuilder.standard().withCredentials(new InstanceProfileCredentialsProvider()).build();
		} else {
			// Construct an instance of AWSCredentials
			AWSCredentials credentials = new BasicAWSCredentials(myAccessKeyId, mySecretKey);
			// Give Configuration
			ClientConfiguration cc = new ClientConfiguration();
			if (((proxy == null) && (port == null)) || ((proxy.length() == 0) && (port == 0))) {
				s3Obj = new AmazonS3Client(credentials);
			} else {
				cc.setProxyHost(proxy);
				cc.setProxyPort(port);
				s3Obj = new AmazonS3Client(credentials, cc);
			}

		}
		// set SSE to decrypt
		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

		try {
			File compressedFile = (new File(compressedFilepath));
			GetObjectRequest request = new GetObjectRequest(bucketName, objectKey);
			s3Obj.getObject(request, compressedFile);
			System.out.println("Your compressed file from S3----" + compressedFilepath);

		} catch (AmazonClientException ace) {
			ace.printStackTrace();
			System.out.println("Exception"+ace);
			System.out.println("Exception: " + ace);
			System.out.println("Error Message: " + ace.getMessage());
			System.out.println("Caught an AmazonClientException, which means the client encountered an internal error while trying to communicate with S3");
		}

	}
	/**
	 * Method to 
	 * 
	 * @param bucketName
	 * @param objectKeyName
	 * @param storageConnectionString
	 * @param fileToUpload
	 * @param s3FileName
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws StorageException
	 */
	
	public void restoreAzureFile(String bucketName,String objectKeyName,String storageConnectionString,File fileToUpload,String s3FileName)
			throws IOException, URISyntaxException, StorageException {
		
		try {
			CloudStorageAccount account = CloudStorageAccount.parse(storageConnectionString);
			CloudBlobClient serviceClient = account.createCloudBlobClient();

			// Container name must be lower case.Create if not exist
			CloudBlobContainer container = serviceClient.getContainerReference(objectKeyName);
			// container.createIfNotExists();

			// Upload an image file.
			CloudBlockBlob blob = container.getBlockBlobReference((fileToUpload.getName()));
			//File sourceFile = new File(srcCompressedFileName);
			//blob.upload(new FileInputStream(srcCompressedFileName), srcCompressedFileName.length());

			// Download the image file.
			File destinationFile = new File(fileToUpload.getParentFile(), s3FileName);
			blob.downloadToFile(destinationFile.getAbsolutePath());
			
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
	 * 
	 * @param myAccessKeyId
	 * @param mySecretKey
	 * @param bucketName
	 * @param objectKey
	 * @param proxy
	 * @param port
	 * @param objectKeyName
	 * @param compressedFilepath
	 * @throws IOException
	 */
	public void notEncryptedDownloadFromS3(String myAccessKeyId, String mySecretKey, String bucketName,
			String objectKey, String proxy, Integer port, String objectKeyName, String compressedFilepath)
			throws IOException {
		AmazonS3 s3Obj;
		if (myAccessKeyId == "NA" && mySecretKey == "NA") {
			s3Obj = AmazonS3ClientBuilder.standard().withCredentials(new InstanceProfileCredentialsProvider()).build();
		} else {
			// Construct an instance of AmazonS3Client

			AWSCredentials credentials = new BasicAWSCredentials(myAccessKeyId, mySecretKey);
			// Give Configuration
			int portsUse = 0;
			ClientConfiguration cc = new ClientConfiguration();
			if (((proxy == null) && (port == null)) || ((proxy.length() == 0) && (port == 0))) {
				s3Obj = new AmazonS3Client(credentials);
			} else {

				cc.setProxyHost(proxy);
				cc.setProxyPort(port);
				s3Obj = new AmazonS3Client(credentials, cc);

			}

		}

		try {
			File compressedFile = (new File(compressedFilepath));
			GetObjectRequest request = new GetObjectRequest(bucketName, objectKey);
			s3Obj.getObject(request, compressedFile);
			System.out.println("Your compressed file from S3----" + compressedFilepath);

		} catch (AmazonClientException ace) {
			ace.printStackTrace();
			System.out.println("Exception"+ace);
			System.out.println("Exception: " + ace);
			System.out.println("Error Message: " + ace.getMessage());
			System.out.println("Caught an AmazonClientException, which means the client encountered an internal error while trying to communicate with S3");
			
		}

	}

	/**
	 * Decompression method using gzip
	 * 
	 * @param source_compressed_filepath
	 * @param destinaton_decompressed_filepath
	 */
	public void decompressGzipFile(String source_compressed_filepath, String destinaton_decompressed_filepath) {
		byte[] buffer = new byte[1024];
		FileInputStream fileInput = null;
		FileOutputStream fileOutputStream = null;
		GZIPInputStream gzipInputStream = null;
		try {
			fileInput = new FileInputStream(source_compressed_filepath);
			fileOutputStream = new FileOutputStream(destinaton_decompressed_filepath);
			gzipInputStream = new GZIPInputStream(fileInput);
			int len;
			while ((len = gzipInputStream.read(buffer)) != -1) {
				fileOutputStream.write(buffer, 0, len);
			}
			System.out.println("The file " + source_compressed_filepath + " was Decompressed successfully: "
					+ destinaton_decompressed_filepath);
		} catch (IOException ex) {
			System.out.println("Exception: " + ex);
			System.out.println("Error Message: " + ex.getMessage());
			System.out.println(" Error in file Decompression " + source_compressed_filepath);
			
		} finally {
			// close resources
			try {
				fileOutputStream.close();
				gzipInputStream.close();
			} catch (IOException e) {
				System.out.println("Exception: " + e);
				System.out.println("Error Message: " + e.getMessage());
				
			}
		}

	}

	public void CreateServer() {

	}

	public void importCsv(String zkDetail, String solrCollection, File file, int flush, List<String> dateFields,
			List<String> longFields) throws IOException {
		long startTime = System.currentTimeMillis();

		String[] incidentAggegateKeys = { "protocolDate", "sourceLocation", "IncidentReportId", "spName",
				"protocolType", "productDescription", "protocolName", "protocolNo", "companyName", "aggregateKey",
				"protocolId", "country", "revisionNo" };

		String[] incidentReportsKeys = { "SeverityTypeId", "ProductRetailCompanyId", "ProductLocationId",
				"ProductCategoryId", "ProductCompanyName", "IncidentDate", "ProductModelName",
				"UserPlansToContactManufacturer", "ProductCategoryPublicName", "RelationshipTypeId",
				"ProductManufacturerCompanyId", "ProductManufacturerName", "ProductUPCCode", "LocalePublicName",
				"VictimGenderId", "SeverityTypeDescription", "LocaleDescription", "hasProtocol", "ProductCityName",
				"ProductPurchasedDateStr", "ProductStateProvince", "LocaleId", "IncidentReportId",
				"ProductSerialNumber", "VictimGenderDescription", "protocolIds", "ProductZipCode",
				"ProductModifiedBeforeIncident", "IncidentReportPublicationDate", "ProductAddressLine1",
				"SourceTypeDescription", "VictimGenderPublicName", "ProductAddressLine2", "ProductAddressLine3",
				"IncidentDateStr", "IncidentReportNumber", "RelationshipTypePublicName",
				"IncidentReportPublicationDateStr", "ProductBrandName", "VictimAgeInMonths",
				"IncidentProductDescription", "RelationshipTypeDescription", "ProductCategoryDescription",
				"AnswerExplanation", "CompanyCommentsExpanded", "SourceTypeId", "UserContactedManufacturer",
				"IncidentReportDateStr", "ProductDamagedBeforeIncident", "UserStillHasProduct",
				"ProductPrimaryPhoneNumber", "ProductPurchasedDate", "IncidentReportDate",
				"ManufacturerNotificationDate", "IncidentDescription", "ProductRetailCompanyName",
				"SeverityTypePublicName", "SourceTypePublicName", "ManufacturerNotificationDateStr" };

		String[] regulation = { "productsPotentiallyCovered", "inForceDate", "regulationName", "locationOrCountry",
				"locationOrCountryCopy", "alertSummary", "proposedDateStr", "date", "dateStr", "regulationId",
				"regulatoryTopics", "inForceDateStr", "proposedDate", "alertHeader", "productsCovered",
				"productsCoveredCopy", "chemicalSubstances" };

		int i = 0;
		CloudSolrClient cloudSolrClientObj = new CloudSolrClient(zkDetail);
		String[] keyValue=null;
		if (solrCollection.equals("Amazon_IncidentAggregate")) {
			keyValue=incidentAggegateKeys;
		} else
			if (solrCollection.equals("Amazon_Regulations")) {
				keyValue=regulation;
			} else
				if (solrCollection.equals("Amazon_Protocols")) {
					//keyValue=protocols;
				} else
					if (solrCollection.equals("Amazon_IncidentReports")) {
						keyValue=incidentReportsKeys;
					}

		try {
			cloudSolrClientObj.setDefaultCollection(solrCollection);
			Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
			// UpdateRequest req = new UpdateRequest();
			List<String> keyList = Arrays.asList(keyValue);
			SimpleDateFormat simpleDate = new SimpleDateFormat("MMM dd, yyyy HH:mm:ss a");
			CSVReader csvReader = null;
			try {
				csvReader = buildCSVReader(file, 0);
			} catch (final UnsupportedEncodingException e1) {
				throw new Exception(e1);
			}

			String[] nextLine;
			while ((nextLine = csvReader.readNext()) != null) {
				// nextLine[] is an array of values from the line
				SolrInputDocument aSolrInputDocument = new SolrInputDocument();
				for (int keyIndex = 0; keyIndex < keyList.size(); keyIndex++) {
					String key = keyList.get(keyIndex);
					// do nothing
					if (dateFields.contains(key)) {
						Date date = simpleDate.parse(nextLine[keyIndex]);
						aSolrInputDocument.addField(key, date);
					} else {
						// default as Text
						aSolrInputDocument.addField(key, nextLine[keyIndex]);
					}
				} // for loop - each document.
					// docs.add(aSolrInputDocument);
				cloudSolrClientObj.add(aSolrInputDocument);
				i++;
				try {
					if (i % flush == 0) {
						// req.setAction(UpdateRequest.ACTION.COMMIT, false,
						// false);
						// cloudSolrClientObj.add(docs);
						cloudSolrClientObj.commit();
						System.out.println("Your data is still uploading, " + i + " : docs uploaded");
						// docs.clear();
					}
					// req.add(docs);
					// req.process(cloudSolrObj);
				} catch (Exception e) {
					System.out.println(" Exception in Reading Document " + e.getMessage());
				} finally {
					docs.clear();
				}
			} // while loop

			// last few documents
			// req.setAction(UpdateRequest.ACTION.COMMIT, false, false);
			// cloudSolrClientObj.add(docs);
			cloudSolrClientObj.commit();
			System.out
					.println("Total Run time to import= " + ((System.currentTimeMillis() - startTime) / 1000) + " Sec");

		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(" Exception in Importing File through Csv " + e);
			System.out.println(" Exception in Importing File through Csv " + e.getMessage());
		}
		cloudSolrClientObj.close();
	}

	private static CSVReader buildCSVReader(final File batchFile, final int offset) throws Exception {
		CSVReader csvReader = null;
		try {
			// , is for separation | " is quotation char | \\ is escape char for
			// both
			// separation & quotation, offset - row number to start reading,
			// true is for strictQuotes.
			csvReader = new CSVReader(new InputStreamReader(new FileInputStream(batchFile.getAbsolutePath()), "UTF-8"),
					',', '"', '\\', offset, false);

		} catch (final FileNotFoundException e) {
			e.printStackTrace();
			throw new Exception(e);
			
		}
		return csvReader;
	}

	/**
	 * method to import data from s3 to solr collection
	 * 
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws ParseException
	 * @throws SolrServerException
	 * @throws java.text.ParseException
	 */
	public void importDataToCollection(String zkDetail, String solrCollection, File file, Integer flustCounter,
			List<String> dateFields, List<String> longFields)
			throws FileNotFoundException, IOException, ParseException, SolrServerException, java.text.ParseException {
		long startTime = System.currentTimeMillis();
		FileReader reader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(reader);
		ObjectMapper mapper = new ObjectMapper(factory);
		String line;
		CloudSolrClient solrCloudClientObj = new CloudSolrClient(zkDetail);
		SimpleDateFormat simpleDate = new SimpleDateFormat("MMM dd, yyyy HH:mm:ss a");
		try {
			solrCloudClientObj.setDefaultCollection(solrCollection);
			// Collection<SolrInputDocument> docs = new
			// ArrayList<SolrInputDocument>();
			// UpdateRequest req = new UpdateRequest();
			int i = 0;
			while ((line = bufferedReader.readLine()) != null) {
				ObjectNode objectNode = mapper.readValue(line, ObjectNode.class);
				SolrInputDocument aSolrInputDocument = new SolrInputDocument();
				Iterator<Entry<String, JsonNode>> records = objectNode.fields();
				while (records.hasNext()) {
					Entry<String, JsonNode> record = records.next();
					String key = record.getKey();
					/*
					 * if (!key.equals("_version_"))
					 * aSolrInputDocument.addField(key,
					 * record.getValue().asText());
					 */
					if (!key.equals("_version_")) {
						// do nothing
						if (dateFields.contains(key)) {
							// date handling
							// can move outside Jan 21, 2016 7:07:00 dd, yyyy
							// HH:mm:ss a
							// String[] dateFormats = {"MMM dd, yyyy HH:mm:ss
							// a"," EEE MMM dd, yyyy HH:mm:ss a"};
							Date date = simpleDate.parse(record.getValue().asText());
							aSolrInputDocument.addField(key, date);
						} else if (longFields.contains(key)) {
							// long data type fields future.
							aSolrInputDocument.addField(key, record.getValue().asLong());
						} else {
							// default as Text
							aSolrInputDocument.addField(key, record.getValue().asText());
						}
					}
				}
				// docs.add(aSolrInputDocument);
				solrCloudClientObj.add(aSolrInputDocument);
				i++;
				try {

					if (i % (flustCounter.intValue()) == 0) {
						// solrCloudClientObj.add(docs);
						solrCloudClientObj.commit();
						// req.setAction(UpdateRequest.ACTION.COMMIT, false,
						// false);
						// docs.clear();
						System.out.println("Your data is still uploading, " + i + " : docs uploaded");
					}
					// req.add(docs);
					// req.process(cloudSolrObj);

				} catch (Exception e) {
					e.printStackTrace();
					System.out.println(" Exception in Importing json " + e);
					System.out.println(" Exception in Reading Document " + e.getMessage());

				} finally {
					// docs.clear();
				}
			}
			// solrCloudClientObj.add(docs);
			solrCloudClientObj.commit();
			// req.setAction(UpdateRequest.ACTION.COMMIT, false, false);
			System.out
					.println("Total Run time to import= " + ((System.currentTimeMillis() - startTime) / 1000) + " Sec");
			reader.close();

		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			System.out.println("Exception: " + e);
			System.out.println("Error Message: " + e.getMessage());
		}
		solrCloudClientObj.close();
	}

	/**
	 * method to delete all records from particular collection
	 * 
	 * @param zkDetail
	 * @param solrCollection
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public void deleteRows(String zkDetail, String solrCollection) throws SolrServerException, IOException {
		CloudSolrClient solr = new CloudSolrClient(zkDetail);
		solr.setDefaultCollection(solrCollection);
		solr.deleteByQuery("*:*");
		solr.commit();
		System.out.println("Deleted");

	}

	/**
	 * method to get Client Side Encrypted (Kms_cmk_id)data from S3
	 * 
	 * @param kms_cmk_id
	 * @param myAccessKeyId
	 * @param mySecretKey
	 * @param bucketName
	 * @param objectKey
	 * @param aws_region
	 * @param compressedFilepath
	 * @param fileNameWithExtrn
	 * @throws IOException
	 */
	public void decryptExportedFile(String kms_cmk_id, String myAccessKeyId, String mySecretKey, String bucketName,
			String objectKey, Regions aws_region, String proxy, int port, String objectKeyName,
			String compressedFilepath, String decompressedFile) throws IOException {

		KMSEncryptionMaterialsProvider materialProvider = new KMSEncryptionMaterialsProvider(kms_cmk_id);
		AmazonS3EncryptionClient decryptionClient;

		// Construct an instance of AmazonS3EncryptionClient
		ClientConfiguration cc = new ClientConfiguration();
		cc.setProxyHost("10.66.80.122");
		cc.setProxyPort(8080);
		AWSCredentials credentials = new BasicAWSCredentials(myAccessKeyId, mySecretKey);
		// creating a new S3EncryptionClient
		decryptionClient = new AmazonS3EncryptionClient(credentials, materialProvider, cc, new CryptoConfiguration())
				.withRegion(Region.getRegion(aws_region));

		System.out.println("Listing objects");

		try {
			ObjectListing objectListing = decryptionClient
					.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix("solrbackup"));
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
				System.out.println(
						" Key ---> " + objectSummary.getKey() + "  " + "(size = " + objectSummary.getSize() + ")");
				objectKey = objectSummary.getKey();
				System.out.println("objectKeys ---> " + objectKey);
			}

			GetObjectRequest request = new GetObjectRequest(bucketName, objectKey);
			S3Object object = decryptionClient.getObject(request);
			S3ObjectInputStream objectContent = object.getObjectContent();

			System.out.println("Your compressed file from S3 " + objectKey);
			File objectnewFile = new File(objectKey);
			String filename = objectnewFile.getName();
			compressedFilepath = "./Result_" + filename;
			/**
			 * InputStream objectData = object.getObjectContent(); // Process
			 * the objectData stream. System.out.println(
			 * "Downloadingloading file: "); objectData.close();
			 **/
			BufferedReader reader = new BufferedReader(new InputStreamReader(objectContent));
			System.out.println("Downloadingloading file: " + compressedFilepath);
			File file = new File(compressedFilepath);
			Writer writer = new OutputStreamWriter(new FileOutputStream(file));
			while (true) {
				String line = reader.readLine();
				if (line == null)
					break;

				writer.write(line + "\n");
			}

			writer.close();

			decompressedFile = "Recover_" + FilenameUtils.removeExtension(filename) + ".txt";
			decompressGzipFile(objectnewFile.getName(), decompressedFile);

		} catch (AmazonClientException ace) {
			ace.printStackTrace();
			System.out.println("Exception"+ace);
			System.out.println(
					"Caught an AmazonClientException, which means the client encountered an internal error while trying to communicate with S3");
			System.out.println("Error Message: " + ace.getMessage());
		}

	}

	/**
	 * method to get data from S3 which is not encrypted
	 * 
	 * @param myAccessKeyId
	 * @param mySecretKey
	 * @param bucketName
	 * @param objectKey
	 * @param aws_region
	 * @param proxy
	 * @param port
	 * @param objectKeyName
	 * @param compressedFilepath
	 * @param decompressedFile
	 * @throws IOException
	 */

	public void getObjectFromS3withoutEncryption(String myAccessKeyId, String mySecretKey, String bucketName,
			String objectKey, Regions aws_region, String proxy, int port, String objectKeyName,
			String compressedFilepath, String decompressedFile) throws IOException {

		AmazonS3Client asc;
		ClientConfiguration cc = new ClientConfiguration();
		cc.setProxyHost(proxy);
		cc.setProxyPort(port);

		// Construct an instance of AmazonS3EncryptionClient
		AWSCredentials credentials = new BasicAWSCredentials(myAccessKeyId, mySecretKey);
		// creating a new S3EncryptionClient

		asc = new AmazonS3Client(credentials, cc);
		System.out.println("Listing objects");

		try {
			ObjectListing objectListing = asc
					.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(objectKeyName));
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
				System.out.println(
						" Key ---> " + objectSummary.getKey() + "  " + "(size = " + objectSummary.getSize() + ")");
				objectKey = objectSummary.getKey();
				System.out.println("objectKeys ---> " + objectKey);
			}

			GetObjectRequest request = new GetObjectRequest(bucketName, objectKey);
			S3Object object = asc.getObject(request);
			S3ObjectInputStream objectContent = object.getObjectContent();

			System.out.println("Your compressed file from S3 " + objectKey);
			File objectnewFile = new File(objectKey);
			String filename = objectnewFile.getName();
			compressedFilepath = "./Result" + filename;
			/**
			 * InputStream objectData = object.getObjectContent(); // Process
			 * the objectData stream. System.out.println(
			 * "Downloadingloading file: "); objectData.close();
			 **/
			BufferedReader reader = new BufferedReader(new InputStreamReader(objectContent));
			System.out.println("Downloadingloading file: " + compressedFilepath);
			File file = new File(compressedFilepath);
			Writer writer = new OutputStreamWriter(new FileOutputStream(file));
			while (true) {
				String line = reader.readLine();
				if (line == null)
					break;

				writer.write(line + "\n");
			}

			writer.close();

			/*
			 * S3Object object = ascobject.getObject(request);
			 * S3ObjectInputStream objectContent = object.getObjectContent();
			 * System.out.println("Your compressed file from S3 " + objectKey);
			 * File objectnewFile = new File(objectKey); String filename=(new
			 * File(objectKey)).getName(); System.out.println(
			 * "Your compressed file from S3 " + objectKey); compressedFilepath
			 * = "./Result_" + filename; BufferedReader reader = new
			 * BufferedReader(new InputStreamReader(objectContent));
			 * System.out.println("Downloadingloading file: " +
			 * compressedFilepath); File file = new File(compressedFilepath);
			 * Writer writer = new OutputStreamWriter(new
			 * FileOutputStream(file)); while (true) { String line =
			 * reader.readLine(); if (line == null) break; writer.write(line );
			 * writer.write(System.lineSeparator()); writer.flush(); }
			 * 
			 * writer.close(); String ff2= (new
			 * File(compressedFilepath).getName()); System.out.println(ff2);
			 * decompressGzipFile(objectnewFile.getName(), ff.getName());
			 */

			File ff = new File(decompressedFile);
			decompressedFile = "recover_" + FilenameUtils.removeExtension(filename) + ".txt";
			decompressGzipFile(objectnewFile.getName(), ff.getName());
		} catch (AmazonClientException ace) {
			ace.printStackTrace();
			System.out.println("Exception"+ace);
			System.out.println(
					
					"Caught an AmazonClientException, which means the client encountered an internal error while trying to communicate with S3");
			System.out.println("Error Message: " + ace.getMessage());
		}

	}

}
