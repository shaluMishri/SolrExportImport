package com.solr.restore;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FilenameUtils;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

//import for regions:  import com.amazonaws.regions.Regions;

/**
 * 
 * @author Shalu_Mishra
 *
 */
public class SolrRecovery {

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();

		ArgumentParser parser = ArgumentParsers.newArgumentParser("SolrRestore").description("Restore solr Collection");
		String cloudStorage = null;
		String zkDetails = null;
		String collectionName = null;
		String bucketName = null;
		String objectKeyName = null;
		Integer flushCounter = 0;
		String s3FileName = null;
		String action = null;
		String accesskey = null;
		String secretkey = null;
		String proxy = null;
		Integer port = 0;
		String datefields = null;
		String longfields = null;
		String localFile = null;
		try {
			parser.addArgument("-cloudStorage", "--cloudStorage").choices("AWS-S3", "AZURE-STORAGE").required(true);
			parser.addArgument("-zkDetails", "--zkDetails").required(true);
			parser.addArgument("-collectionName", "--collectionName").required(true);
			parser.addArgument("-bucketName", "--bucketName").required(true);
			parser.addArgument("-objectKeyName", "--objectKeyName").required(true);
			parser.addArgument("-flushCounter", "--flushCounter").type(Integer.class).setDefault(100).required(true);
			parser.addArgument("-s3FileName", "--s3FileName").required(true);
			parser.addArgument("-action", "--action")
					.choices("RUN-FROM-S3:S3SSE:IMPORT", "RUN-FROM-LOCAL", "RUN-FROM-AZURE-STORAGE").required(true)
					.setDefault("RUN-FROM-LOCAL");
			parser.addArgument("-accesskey", "--accesskey").required(false);
			parser.addArgument("-secretkey", "--secretkey").required(false);
			parser.addArgument("-proxy", "--proxy").required(false);
			parser.addArgument("-port", "--port").setDefault(0).type(Integer.class).required(false);
			parser.addArgument("-datefields", "--datefields").required(false);
			parser.addArgument("-longfields", "--longfields").required(false);
			parser.addArgument("-localFile", "--localFile").required(false);

			cloudStorage = parser.parseArgs(args).getString("cloudStorage");
			zkDetails = parser.parseArgs(args).getString("zkDetails");
			collectionName = parser.parseArgs(args).getString("collectionName");
			bucketName = parser.parseArgs(args).getString("bucketName");
			objectKeyName = parser.parseArgs(args).getString("objectKeyName");
			flushCounter = Integer.parseInt(parser.parseArgs(args).getString("flushCounter"));
			s3FileName = parser.parseArgs(args).getString("s3FileName");
			action = parser.parseArgs(args).getString("action");
			accesskey = parser.parseArgs(args).getString("accesskey");
			secretkey = parser.parseArgs(args).getString("secretkey");
			proxy = parser.parseArgs(args).getString("proxy");
			port = Integer.parseInt(parser.parseArgs(args).getString("port"));
			datefields = parser.parseArgs(args).getString("datefields");
			longfields = parser.parseArgs(args).getString("longfields");
			localFile = parser.parseArgs(args).getString("localFile");

		} catch (ArgumentParserException are) {
			// TODO: handle exception
			are.printStackTrace();
			System.out.println("Exception: " + are);
			System.out.println("Error message: " + are.getMessage());
			System.exit(-1);

		}

		String decompressedFile = "Recover_" + FilenameUtils.removeExtension(s3FileName) + ".json";
		String objectkey = objectKeyName + "/" + s3FileName;
		SolrRecoveryHelper solrExportHelper = new SolrRecoveryHelper();
		List<String> myList = new ArrayList<String>(Arrays.asList(datefields.split(",")));
		List<String> myListlongFiels = new ArrayList<String>(Arrays.asList(longfields.split(",")));
		String storageConnectionString = "DefaultEndpointsProtocol=https;" + "AccountName=" + bucketName + ";"
				+ "AccountKey=" + accesskey;
		System.out.println("Action is " + action);

		try {
			if (cloudStorage.equals("AWS-S3")) {
				if (action.equals("RUN-FROM-S3:S3SSE:IMPORT")) {
					// Step # 1 & 2 decryt and download the file from S3 and
					// import
					String subStr = "RUN-FROM-S3:S3SSE:IMPORT";
					if ((subStr.substring(12, 17)).equals("S3SSE")) {
						solrExportHelper.decryptSSEDownloadFromS3(accesskey, secretkey, bucketName, objectkey, proxy,
								port, objectKeyName, s3FileName);
						} else {
						solrExportHelper.notEncryptedDownloadFromS3(accesskey, secretkey, bucketName, objectkey, proxy,
								port, objectKeyName, s3FileName);
								}

					// run locally
				} else if (action.equals("RUN-FROM-LOCAL")) {
					File fileToUpload = new File(localFile);
					solrExportHelper.importDataToCollection(zkDetails, collectionName, fileToUpload, flushCounter,
							myList, myListlongFiels);
				}
			} else if (cloudStorage.equals("AZURE-STORAGE")) {
				File fileToUploadAzure = new File(s3FileName);
				solrExportHelper.restoreAzureFile(bucketName, objectKeyName, storageConnectionString, fileToUploadAzure,
						s3FileName);
			}
			// Decompress
			solrExportHelper.decompressGzipFile(s3FileName, decompressedFile);
			try {
				if ((flushCounter.intValue()) != 0) {
					File fileToUpload = new File(decompressedFile);
					solrExportHelper.importDataToCollection(zkDetails, collectionName, fileToUpload, flushCounter,
							myList, myListlongFiels);
				}
			} catch (Exception ace) {
				ace.printStackTrace();
				System.out.println("Flush Counter value is " + ace.getMessage());
				System.out.println(" Exception " + ace);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception" + e);
			System.out.println("Exception" + e.getMessage());
		}
		System.out.println("Total Run time = " + ((System.currentTimeMillis() - startTime) / 1000) + " Sec");
		System.exit(0);
	}

}
