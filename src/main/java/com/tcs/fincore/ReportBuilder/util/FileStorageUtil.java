package com.tcs.fincore.ReportBuilder.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * FIXED: Thread-safe HDFS file storage utility with proper error handling and synchronization
 */
@Slf4j
public class FileStorageUtil {
    
//    // Lock per path to allow concurrent writes to different files
//    private static final java.util.concurrent.ConcurrentHashMap<String, Lock> PATH_LOCKS =
//        new java.util.concurrent.ConcurrentHashMap<>();


    // ============================================================
// PATH_LOCKS cleaned up after each use to prevent unbounded
// memory growth across tens of thousands of HDFS writes.
// ============================================================

    public static void writeToFile(byte[] data, FileSystem fs, Path filePath) throws IOException {
        if (data == null || data.length == 0)
            throw new IllegalArgumentException("Data cannot be null or empty");
        if (fs == null)
            throw new IllegalArgumentException("FileSystem cannot be null");
        if (filePath == null)
            throw new IllegalArgumentException("FilePath cannot be null");

        String pathKey = filePath.toString();
        //  computeIfAbsent creates the lock; we remove it after use.
        ReentrantLock lock = (ReentrantLock) PATH_LOCKS.computeIfAbsent(
                pathKey, k -> new ReentrantLock());

        lock.lock();
        try {
            writeWithRetry(data, fs, filePath, 3);
            log.debug("Successfully wrote {} bytes to HDFS: {}", data.length, filePath);
        } finally {
            lock.unlock();
            // Remove the lock only if no other thread is waiting on it.
            // ReentrantLock.hasQueuedThreads() is safe here because we still hold
            // the lock key in the map; a concurrent writer will re-create it if needed.
            if (!lock.hasQueuedThreads()) {
                PATH_LOCKS.remove(pathKey, lock);
            }
        }
    }
    // Change field to use ReentrantLock (not the interface Lock) so hasQueuedThreads() is accessible:
    private static final ConcurrentHashMap<String, ReentrantLock> PATH_LOCKS =
            new ConcurrentHashMap<>();

    /**
     * Write with automatic retry logic for transient HDFS failures
     */
    private static void writeWithRetry(byte[] data, FileSystem fs, Path filePath, int maxRetries) 
            throws IOException {
        
        IOException lastException = null;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                // Ensure parent directory exists
                Path parentDir = filePath.getParent();
                if (parentDir != null && !fs.exists(parentDir)) {
                    fs.mkdirs(parentDir);
                    log.debug("Created parent directory: {}", parentDir);
                }

                // Write file atomically with proper resource management
                try (FSDataOutputStream outputStream = fs.create(filePath, true)) {
                    outputStream.write(data);
                    outputStream.hflush(); // Flush to HDFS
                    outputStream.hsync();  // Ensure data is persisted
                }
                
                // Verify write succeeded
                if (!fs.exists(filePath)) {
                    throw new IOException("File write completed but file does not exist: " + filePath);
                }
                
                long fileSize = fs.getFileStatus(filePath).getLen();
                if (fileSize != data.length) {
                    throw new IOException(String.format(
                        "File size mismatch. Expected: %d, Actual: %d", data.length, fileSize));
                }
                
                return; // Success
                
            } catch (IOException e) {
                lastException = e;
                log.warn("HDFS write attempt {}/{} failed for {}: {}", 
                    attempt, maxRetries, filePath, e.getMessage());
                
                if (attempt < maxRetries) {
                    try {
                        // Exponential backoff: 100ms, 200ms, 400ms
                        Thread.sleep(100L * (1L << (attempt - 1)));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Write interrupted during retry", ie);
                    }
                    
                    // Clean up partial file if it exists
                    try {
                        if (fs.exists(filePath)) {
                            fs.delete(filePath, false);
                            log.debug("Deleted partial file before retry: {}", filePath);
                        }
                    } catch (IOException cleanupEx) {
                        log.warn("Failed to cleanup partial file: {}", cleanupEx.getMessage());
                    }
                }
            }
        }
        
        // All retries failed
        log.error("Failed to write to HDFS after {} attempts: {}", maxRetries, filePath);
        throw new IOException("HDFS write failed after " + maxRetries + " attempts", lastException);
    }

    /**
     * Check if file exists in HDFS (useful for validation)
     */
    public static boolean fileExists(FileSystem fs, Path filePath) {
        try {
            return fs.exists(filePath);
        } catch (IOException e) {
            log.error("Error checking file existence: {}", filePath, e);
            return false;
        }
    }

    /**
     * Delete file from HDFS with proper error handling
     */
    public static boolean deleteFile(FileSystem fs, Path filePath) {
        try {
            if (fs.exists(filePath)) {
                boolean deleted = fs.delete(filePath, false);
                if (deleted) {
                    log.info("Deleted file from HDFS: {}", filePath);
                }
                return deleted;
            }
            return false;
        } catch (IOException e) {
            log.error("Error deleting file: {}", filePath, e);
            return false;
        }
    }
}