package com.solr.backup;

import java.io.File;
import com.solr.backup.SolrBackupHelper;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

/**
 * Query Solr, upload compressed & encrypted output to AWS-S3 bucket/Azure-Storage.
 * 
 * @author Shalu_Mishra
 *
 */
public class SolrBackup {

	public static void main(String[] args) throws Exception {
		// start time of run
		long startTime = System.currentTimeMillis();
		ArgumentParser parser = ArgumentParsers.newArgumentParser("SolrBackup").description("Backup of solr Collection");
		String cloudStorage = null;
		String zkDetails = null;
		Integer fetchSize = 0;
		String collectionName = null;
		String uniquekey = null;
		String queryArgs = null;
		String bucketName = null;
//		Integer retentionPeriod = 0;
		String objectKeyName = null;
		String action = null;
		String accesskey = null;
		String secretkey = null;
		String proxy = null;
		Integer port = 0;
		try {
			parser.addArgument("-cloudStorage", "--cloudStorage").choices("AWS-S3", "AZURE-STORAGE").required(true);
			parser.addArgument("-zkDetails", "--zkDetails").required(true);
			parser.addArgument("-fetchSize", "--fetchSize").required(true);
			parser.addArgument("-collectionName", "--collectionName").required(true);
			parser.addArgument("-uniquekey", "--uniquekey").required(true);
			parser.addArgument("-queryArgs", "--queryArgs").required(true);
			parser.addArgument("-bucketName", "--bucketName").required(true);
		//	parser.addArgument("-retentionPeriod", "--retentionPeriod").setDefault(7).type(Integer.class).required(false);
			parser.addArgument("-objectKeyName", "--objectKeyName").required(true);
			parser.addArgument("-action", "--action").choices("BACKUP-TO-S3", "BACKUP-TO-LOCAL","BACKUP-TO-AZURE-STORAGE").required(true)
			.setDefault("BACKUP-TO-LOCAL");
			parser.addArgument("-accesskey", "--accesskey").required(false);
			parser.addArgument("-secretkey", "--secretkey").required(false);
			parser.addArgument("-proxy", "--proxy").required(false);
			parser.addArgument("-port", "--port").setDefault(0).type(Integer.class).required(false);

			cloudStorage = parser.parseArgs(args).getString("cloudStorage");
			zkDetails = parser.parseArgs(args).getString("zkDetails");
			fetchSize = Integer.parseInt(parser.parseArgs(args).getString("fetchSize"));
			collectionName = parser.parseArgs(args).getString("collectionName");
			uniquekey = parser.parseArgs(args).getString("uniquekey");
			queryArgs = parser.parseArgs(args).getString("queryArgs");
			bucketName = parser.parseArgs(args).getString("bucketName");
	//		retentionPeriod = Integer.parseInt(parser.parseArgs(args).getString("retentionPeriod"));
			objectKeyName = parser.parseArgs(args).getString("objectKeyName");
			action = parser.parseArgs(args).getString("action");
			accesskey = parser.parseArgs(args).getString("accesskey");
			secretkey = parser.parseArgs(args).getString("secretkey");
			proxy = parser.parseArgs(args).getString("proxy");
			port = Integer.parseInt(parser.parseArgs(args).getString("port"));

		} catch (ArgumentParserException e) {

			System.out.println("Exception: " + e);
			System.out.println("Error Message: " + e.getMessage());
			System.exit(-1);
		}

		System.out.println(" 1. 	cloudStorage				" + cloudStorage);
		System.out.println(" 2. 	ZK Details example			" + zkDetails);
		System.out.println(" 3. 	fetch Size example			" + fetchSize);
		System.out.println(" 4. 	Name of collection			" + collectionName);
		System.out.println(" 5. 	Name of unique key			" + uniquekey);
		System.out.println(" 6. 	Query						" + queryArgs);
		System.out.println(" 7. 	bucketName/AccounrName Name	" + bucketName);
	//	System.out.println(" 8. 	expirationInDays			" + retentionPeriod);
		System.out.println(" 9. 	objectKeyName				" + objectKeyName);
		System.out.println(" 10. 	action						" + action);
		System.out.println(" 11. 	accesskey					" + accesskey);
		System.out.println(" 12. 	secretkey					" + secretkey);
		System.out.println(" 13. 	proxy						" + proxy);
		System.out.println(" 14. 	port						" + port);
		String storageConnectionString ="DefaultEndpointsProtocol=https;"+
										 "AccountName="+bucketName+";"+
										 	"AccountKey="+accesskey;

		String originalfileName = "export_" + collectionName + "_" + startTime;
		String srcfileName = originalfileName + ".json";
		String compressedFileName = originalfileName + ".gz";
		String objectKey = objectKeyName + "/";
		System.out.println(" Action is : " + action);
		System.out.println("You Backup file for Collection " + collectionName + " is: " + srcfileName);

		if (cloudStorage.equals("AWS-S3")) {
			// AWS-S3
			
			try {
				SolrBackupHelper solrBackupHelper = new SolrBackupHelper();

				if (action.equals("BACKUP-TO-S3")) {
					//Step # 1 #Query the Solr end point and write output to file
					solrBackupHelper.queryAndExportToFile(srcfileName, zkDetails, (fetchSize.intValue()),
							collectionName, uniquekey, queryArgs);

					//Step # 2 #compress the file output of solr.
					solrBackupHelper.compressGzipFile(srcfileName, compressedFileName);
					File compressedFileObj = new File((new File(compressedFileName)).getName());

					//Step # 3 #SSEencrypt the compressed file and upload to s3 bucket.(retentionPeriod.intValue())
					solrBackupHelper.encryptSSEExportedFile(bucketName, objectKey, accesskey, secretkey,
							compressedFileObj,  port, proxy, objectKeyName);

					//Step #4 # delete the files from local OS
					solrBackupHelper.delete(compressedFileName,cloudStorage);
				} else
					if (action.equals("BACKUP-TO-LOCAL")) {
						//Step # 1 #Query the Solr end point and write output to file
						solrBackupHelper.queryAndExportToFile(srcfileName, zkDetails, (fetchSize.intValue()),
								collectionName, uniquekey, queryArgs);
						
						//Step # 2 #compress the file output of solr.
						solrBackupHelper.compressGzipFile(srcfileName, compressedFileName);
						System.out.println("You File " + compressedFileName + " is saved Locally ");
					}

			} catch (Exception e) {
				System.out.println("Exception: " + e);
				System.out.println("Error Message: " + e.getMessage());
			}
		} 
		else if (cloudStorage.equals("AZURE-STORAGE")) {
			// AZURE-STORAGE
			try {
				SolrBackupHelper solrBackupHelper = new SolrBackupHelper();

				if (action.equals("BACKUP-TO-AZURE-STORAGE")) {
					//Step # 1 #Query the Solr end point and write output to file
					solrBackupHelper.queryAndExportToFile(srcfileName, zkDetails, (fetchSize.intValue()),
							collectionName, uniquekey, queryArgs);

					//Step # 2 #compress the file output of solr.
					solrBackupHelper.compressGzipFile(srcfileName, compressedFileName);
					File compressedFileObj = new File((new File(compressedFileName)).getName());

					//Step # 3 #SSEencrypt the compressed file and upload to Azure Blob.
					solrBackupHelper.compressedAzureExportedFile(bucketName, objectKeyName, storageConnectionString, compressedFileObj);
					//Step #4 # delete the files from local OS
					solrBackupHelper.delete(compressedFileName,cloudStorage);
				} else
					if (action.equals("BACKUP-TO-LOCAL")) {
						//Step # 1 #Query the Solr end point and write output to file
						solrBackupHelper.queryAndExportToFile(srcfileName, zkDetails, (fetchSize.intValue()),
								collectionName, uniquekey, queryArgs);
						
						//Step # 2 #compress the file output of solr.
						solrBackupHelper.compressGzipFile(srcfileName, compressedFileName);
						System.out.println("You File " + compressedFileName + " is saved Locally ");
					}

			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Exception: " + e);
				System.out.println("Error Message: " + e.getMessage());
			}
		}

		System.out.println("Total Run time = " + ((System.currentTimeMillis() - startTime) / 1000) + " Sec");
		System.exit(0);
	}

}
