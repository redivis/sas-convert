/**
 * Copyright (c) 2015 The Hyve
 * This file is distributed under the MIT License (see accompanying file LICENSE).
 */
package thehyve.sas;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.text.DateFormat;

import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVWriter;

import com.epam.parso.Column;
import com.epam.parso.SasFileProperties;
import com.epam.parso.SasFileReader;
import com.epam.parso.impl.SasFileReaderImpl;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobId;


import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.WriteChannel;

import com.google.cloud.storage.StorageOptions;
import com.google.cloud.ReadChannel;

/**
 * Command-line utility to convert files in SAS7BDAT format to
 * comma-separated format (CSV).
 * Based on the Parso library ({@link http://lifescience.opensource.epam.com/parso.html})
 * and opencsv ({@link http://opencsv.sourceforge.net/})
 *
 * @author gijs@thehyve.nl
 */
public class Convert {

    public static final String USAGE = "Usage: sas-convert <file.sas> [file.csv]\n\nWhen only one filename is supplied, output will be sent to stdout.\n\nOptions:\n\n-o, --only-column-names\n       Only write column names before data rows (default is to write three\n       header lines: labels, names, and formats)\n\n-a, --auto-create-csv\n       Instead of sending output to stdout when only an input filename is\n       provided, this will save it to a file based on the name of the input file\n       (e.g. `sas-convert my-filename.sas7bdat` will produce my-filename.csv).";
    private static final Logger log = LoggerFactory.getLogger(Convert.class);

    public void convert(InputStream in, OutputStream out, OutputStream metadataOut) throws IOException {
        Date start = new Date();
        SasFileReader reader = new SasFileReaderImpl(in);
        CSVWriter writer = new CSVWriter(new OutputStreamWriter(out));
		CSVWriter metadataWriter = new CSVWriter(new OutputStreamWriter(metadataOut));
        Object[] data;
        SasFileProperties properties = reader.getSasFileProperties();
        log.info("Reading file " + properties.getName());
        log.info(properties.getRowCount() + " rows.");
        List<Column> columns = reader.getColumns();
		String[] metadata = new String[columns.size()];
        String[] outData = new String[columns.size()];
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

		// Writing column names
		for(int i=0; i < columns.size(); i++) {
            metadata[i] = columns.get(i).getName();
        }
		metadataWriter.writeNext(metadata);
        // Writing column labels
        for(int i=0; i < columns.size(); i++) {
            metadata[i] = columns.get(i).getLabel();
        }
        metadataWriter.writeNext(metadata);
		log.info("Done writing metadata.");
		metadataWriter.flush();
		metadataWriter.close();

        // if (!onlyColumnNames) {
        //     // Writing column format
        //     for(int i=0; i < columns.size(); i++) {
        //         outData[i] = columns.get(i).getFormat();
        //     }
        //     writer.writeNext(outData);
        // }

		for(int i=0; i < columns.size(); i++) {
            outData[i] = columns.get(i).getName();
        }
		writer.writeNext(outData);
        try {
            log.info("Writing data...");
            long rowCount = 0;
            while((data = reader.readNext()) != null) {
                assert(columns.size() == data.length);
                for(int i=0; i < data.length; i++) {
					if (data[i] instanceof Date){
						outData[i] = data[i] == null ? "" : dateFormat.format(data[i]);
					}
					else {
						outData[i] = data[i] == null ? "" : data[i].toString();
					}

                }
                writer.writeNext(outData);
                rowCount++;
				// if (rowCount > 2){
				// 	break;
				// }
            }
            log.info("Done writing data.");
            log.info(rowCount + " rows written.");
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
        Date end = new Date();
        log.info("Converting took {} seconds.", (end.getTime() - start.getTime())/1000);
    }

    public static void main(String[] args) {
        Options options = new Options();
		Storage storage = StorageOptions.getDefaultInstance().getService();

        options.addOption("h", "help", false, "Help");
        options.addOption("o", "only-column-names", false, "Only column names");
        options.addOption("a", "auto-create-csv", false, "Auto create CSV");
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cl = parser.parse(options, args);
            if (cl.hasOption("help")) {
                System.err.printf(USAGE + "\n");
                return;
            }
            List<String> argList = cl.getArgList();
            if (argList.size() < 3) {
                System.err.printf("Too few parameters.\n\n" + USAGE + "\n");
                return;
            } else if (argList.size() > 3) {
                System.err.printf("Too many parameters.\n\n" + USAGE + "\n");
                return;
            }
            try {
                String in_filename = argList.get(0);
				String out_filename = argList.get(1);
				String metadata_filename = argList.get(2);
				String bucketName = in_filename.replace("gs://", "").split("/")[0];
				String objectName = in_filename.replace("gs://" + bucketName + "/", "");

				String outBucketName = out_filename.replace("gs://", "").split("/")[0];
				String outObjectName = out_filename.replace("gs://" + bucketName + "/", "");
				log.info("BucketName " + bucketName);
				log.info("ObjectName " + objectName);
				// gs://redivis-data-dev/truven_dental/dnmcn073.sas7bdat
				ReadChannel reader = storage.reader(bucketName, objectName);
				InputStream fin = Channels.newInputStream(reader);
				OutputStream metadataOut = new FileOutputStream(metadata_filename);
                // FileInputStream fin = new FileInputStream(in_filename);

				BlobId blobId = BlobId.of(outBucketName, outObjectName);
				BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/csv").build();
				WriteChannel writer = storage.writer(blobInfo);
				// writer.setChunkSize(33554432 / 2);

				OutputStream fout = Channels.newOutputStream(writer);


                // if (argList.size() > 1) {
                //     String out_filename = argList.get(1);
                //     log.info("Writing to file: {}", out_filename);
                //     fout = new FileOutputStream(out_filename);
                // } else if (cl.hasOption("auto-create-csv")) {
                //     String out_filename;
                //     if (in_filename.contains(".")) {
                //         out_filename = in_filename.replaceAll("\\.[^.]*$", ".csv");
                //     } else {
                //         out_filename = in_filename.concat(".csv");
                //     }
                //     log.info("Writing to file: {}", out_filename);
                //     fout = new FileOutputStream(out_filename);
                // } else {
                //     log.info("Writing to stdout.");
                //     fout = System.out;
                // }
                Convert converter = new Convert();
                converter.convert(fin, fout, metadataOut);
                fin.close();
                fout.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (ParseException e) {
            System.err.printf(USAGE + "\n");
            e.printStackTrace();
            return;
        }
    }

}
