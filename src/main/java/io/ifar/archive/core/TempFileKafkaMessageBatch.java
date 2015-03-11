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
import java.nio.charset.Charset;
import java.util.*;

public class TempFileKafkaMessageBatch implements KafkaMessageBatch {
    final static Logger LOG = LoggerFactory.getLogger(TempFileKafkaMessageBatch.class);

    private final Map<String, FileBackedWriter> tempWriters = new HashMap<>();

    /**
     * Set of entries that (may) have been written in S3; needed to handle interruptions
     * during batch write, atomic rollback.
     */
    private Set<String> currentBatchFilesWritten = new HashSet<>();

    private String bucket;
    private AmazonS3Client s3Client;

    public TempFileKafkaMessageBatch(String bucket, AmazonS3Client s3Client) {
        this.bucket = bucket;
        this.s3Client = s3Client;
    }

    @Override
    public void addMessageToArchiveQueue(String archiveBatchKey, byte[] message) throws Exception {
        FileBackedWriter writer = tempWriters.get(archiveBatchKey);
        if (writer == null) {
            writer = FileBackedWriter.create(archiveBatchKey);
            tempWriters.put(archiveBatchKey, writer);
        }
        writer.writeLine(message);
    }

    @Override
    public void writeToArchive() throws Exception {
        if (tempWriters.isEmpty()) {
            return;
        }
        try {
            // Let's actually do this in two loops, to make it more likely things are more batch-atomic;
            // first, flush and close all temporary files; second, try to upload.
            LOG.debug("writeToArchive: closing {} temp files.", tempWriters.size());
            for (FileBackedWriter w : tempWriters.values()) {
                w.close();
            }

            LOG.debug("writeToArchive: uploading {} temp files.", tempWriters.size());
            Iterator<Map.Entry<String, FileBackedWriter>> it = tempWriters.entrySet().iterator();
            while (it.hasNext()) {
                final FileBackedWriter tempWriter = it.next().getValue();
                final String key = tempWriter.getKey();
                final File tempFile = tempWriter.getFile();
                if (LOG.isDebugEnabled()) { // just because file.length() causes I/O access
                    LOG.debug("Writing temp file {} ({} bytes) to {}", tempFile.getName(), tempFile.length(), key);
                }
                PutObjectRequest request = new PutObjectRequest(bucket, key, tempFile);
                // add first, so that set contains anything that may have been uploaded
                currentBatchFilesWritten.add(key);
                s3Client.putObject(request);
                LOG.debug("Completed put of {}", key);

                it.remove();

                if (tempFile.delete())
                    LOG.debug("Deleted temp file {}", tempFile.getName());
                else
                    LOG.warn("Failed to delete temp file {}", tempFile.getName());
            }
        } catch(AbortedException e) {
            LOG.info("AbortedException thrown while writing S3 files; throwing InterruptedException", e);
            throw new InterruptedException(e.getMessage());
        }
    }

    @Override
    public void deleteArchiveBatch() throws InterruptedException {
        Iterator<Map.Entry<String, FileBackedWriter>> it = tempWriters.entrySet().iterator();
        while (it.hasNext()) {
            FileBackedWriter tempWriter = it.next().getValue();
            it.remove();

            final String key = tempWriter.getKey();

            try {
                tempWriter.close();
            } catch (IOException e) {
                LOG.warn("Expected writer to temp file " + key + " to be open, but was closed", e);
            }
            File tempFile = tempWriter.getFile();
            if (tempFile.delete())
                LOG.debug("Deleted temp file {}", tempFile.getName());
            else
                LOG.warn("Failed to delete temp file {}", tempFile.getName());
        }

        // and then files already uploaded in S3, if any
        Iterator<String> s3it = currentBatchFilesWritten.iterator();
        while (it.hasNext()) {
            String fileKey = s3it.next();
            it.remove();
            try {
                DeleteObjectRequest request = new DeleteObjectRequest(bucket, fileKey);
                s3Client.deleteObject(request); // will succeed if object does not exist
            } catch (AbortedException e) {
                LOG.info("AbortedException thrown while deleting S3 files; throwing InterruptedException", e);
                throw new InterruptedException(e.getMessage());
            }
        }
    }

    @Override
    public int size() {
        return tempWriters.size();
    }

    /**
     * Helper class for containing in-flight state of a buffered entry.
     */
    private final static class FileBackedWriter
        implements Closeable
    {
        private final static byte LF = '\n';

        private final String key;
        private final File file;
        private final OutputStream writer;

        private boolean closed;

        private FileBackedWriter(String k, File f, OutputStream w) {
            key = k;
            file = f;
            writer = w;
        }

        public static FileBackedWriter create(String key) throws IOException {
            File tempFile = File.createTempFile(key, null);
            BufferedOutputStream w = new BufferedOutputStream(new FileOutputStream(tempFile));
            return new FileBackedWriter(key, tempFile, w);
        }

        public String getKey() { return key; }
        public File getFile() { return file; }

        public void writeLine(byte[] content) throws IOException {
            writer.write(content);
            writer.write(LF);
        }

        @Override
        public void close() throws IOException {
            // calling close() twice on OutputStream may or may not be problematic, but
            // let's try to reduce noise here
            if (!closed) {
                closed = true;
                writer.close();
            }
        }
    }
}
