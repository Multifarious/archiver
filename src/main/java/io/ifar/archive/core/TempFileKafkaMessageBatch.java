package io.ifar.archive.core;

import com.amazonaws.AbortedException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import io.ifar.archive.S3Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class TempFileKafkaMessageBatch implements KafkaMessageBatch {
    final static Logger LOG = LoggerFactory.getLogger(TempFileKafkaMessageBatch.class);

    private HashMap<String, File> tempFiles = new HashMap<>();
    private HashMap<String, BufferedWriter> tempFileWriters = new HashMap<>();

    private Set<String> currentBatchFilesWritten = new HashSet<>();   // objects in S3

    private S3Configuration s3Configuration;
    private AmazonS3Client s3Client;

    public TempFileKafkaMessageBatch(S3Configuration s3Configuration, AmazonS3Client s3Client) {
        this.s3Configuration = s3Configuration;
        this.s3Client = s3Client;
    }

    @Override
    public void addMessageToArchiveQueue(String archiveBatchKey, String message) throws Exception {
        BufferedWriter writer = tempFileWriters.get(archiveBatchKey);
        if(writer == null) {
            File tempFile = File.createTempFile(archiveBatchKey, null);
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tempFile)));
            tempFiles.put(archiveBatchKey, tempFile);
            tempFileWriters.put(archiveBatchKey, writer);
        }
        writer.write(message);
        writer.newLine();
    }

    @Override
    public void writeToArchive() throws Exception {
        try {
            for (Map.Entry<String, File> tempFileEntry : tempFiles.entrySet()) {
                String key = tempFileEntry.getKey();
                File tempFile = tempFileEntry.getValue();
                BufferedWriter tempFileWriter = tempFileWriters.get(key);
                tempFileWriter.close();
                LOG.debug("Writing temp file {} ({} bytes) to {}", tempFile.getName(), tempFile.length(), key);
                PutObjectRequest request = new PutObjectRequest(s3Configuration.getBucket(), key, tempFile);
                s3Client.putObject(request);
                currentBatchFilesWritten.add(key);
                LOG.debug("Completed put of {}", key);
                tempFileWriters.remove(key);
                if(tempFile.delete())
                    LOG.debug("Deleted temp file {}", tempFile.getName());
                else
                    LOG.warn("Failed to delete temp file {}", tempFile.getName());
            }
            return;
        } catch(AbortedException e) {
            LOG.info("AbortedException thrown while writing S3 files; throwing InterruptedException", e);
            throw new InterruptedException();
        }
    }

    @Override
    public void deleteArchiveBatch() throws InterruptedException {
        for(Map.Entry<String, BufferedWriter> entry : tempFileWriters.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                LOG.warn("Expected writer to temp file " + entry.getKey() + " to be open, but was closed", e);
            }
            File tempFile = tempFiles.get(entry.getKey());
            if(tempFile.delete())
                LOG.debug("Deleted temp file {}", tempFile.getName());
            else
                LOG.warn("Failed to delete temp file {}", tempFile.getName());
        }

        try {
            for (String fileKey : currentBatchFilesWritten) {
                DeleteObjectRequest request = new DeleteObjectRequest(s3Configuration.getBucket(), fileKey);
                s3Client.deleteObject(request); // will succeed if object does not exist
            }
        } catch(AbortedException e) {
            LOG.info("AbortedException thrown while deleting S3 files; throwing InterruptedException", e);
            throw new InterruptedException();
        }
    }

    @Override
    public int size() {
        return tempFiles.size();
    }
}
