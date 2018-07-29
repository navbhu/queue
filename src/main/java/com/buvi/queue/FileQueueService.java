package com.buvi.queue;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class FileQueueService implements QueueService {

    private static final HashMap<String, FileQueue> MAP = new HashMap<>();
    private static final String DIR_NAME = System.getProperty("user.home")+"/queue-messages/";
    private static final File FILE_DIR = new File(DIR_NAME);
    private static long time = System.currentTimeMillis();
    private static final long DEFAULT_VISIBILITY = 3000L;
    private static final QueueService INSTANCE = new FileQueueService();
    private static final String FILE_SEPARATOR = System.getProperty("file.separator");
    private static final String LOCK_FILE_NAME = "lock";
    private static final String CONSUMED_FILE_NAME = "consumed";


    static {
        createDirectory(FILE_DIR);
        updateTime();
    }

    private FileQueueService(){
    }

    static QueueService getInstance(){
        return INSTANCE;
    }

    private static void createDirectory(File directory) {
        String directoryName = directory.getName();
        if(directory.exists()) {
            if(directory.isDirectory()){
                log(directoryName + " already exists.");
            } else {
                log(directoryName + " is a file.");
            }
        } else {
            if(directory.mkdirs()) {
                log(directoryName +" has been created");
            } else {
                throw new RuntimeException("Couldn't create directory: " + directoryName);
            }
        }
    }

    private static void lock(File file){
        while(! file.mkdir()){
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void unlock(File file){
        file.delete();
    }

    private static void updateTime(){
        File lock = new File(FILE_DIR.getAbsolutePath()+FILE_SEPARATOR+LOCK_FILE_NAME);
        lock(lock);
        try {
            time = lock.lastModified();
        } finally {
            unlock(lock);
        }
    }

    private FileQueue getOrCreateQueue(String queueName) {
        if( ! MAP.containsKey(queueName)){
            synchronized (MAP){
                if( ! MAP.containsKey(queueName)){
                    MAP.put(queueName, new FileQueue(queueName));
                }
            }
        }
        return MAP.get(queueName);
    }

    @Override
    public void push(String queueName, String message) {
        FileQueue queue = getOrCreateQueue(queueName);
        queue.push(message);
    }

    @Override
    public Message pull(String queueName) {
        FileQueue queue = getOrCreateQueue(queueName);
        return queue.pull();
    }

    @Override
    public void delete(String queueName, String messageId) {
        FileQueue queue = getOrCreateQueue(queueName);
        queue.delete(messageId);
    }

    @Override
    public List<Message> pull(String queueName, int limit) {
        return null;
    }

    private static void log(String msg){
        System.out.println(msg);
    }

    private class FileQueue {

        private AtomicLong counter = new AtomicLong(0L);
        private File queueDir;
        private File consumed;
        private File lockFile;

        private FileQueue(String queueName){
            this.queueDir = new File(DIR_NAME + FILE_SEPARATOR + queueName);
            createDirectory(queueDir);
            this.consumed = new File(queueDir.getAbsolutePath() + FILE_SEPARATOR + CONSUMED_FILE_NAME);
            createDirectory(consumed);
            lockFile = new File(queueDir + FILE_SEPARATOR + LOCK_FILE_NAME);
        }

        private void push(String message) {
            String id = time + "-" + String.format("%015d", counter.getAndIncrement());
            try (FileWriter writer = new FileWriter(queueDir.getAbsolutePath() + FILE_SEPARATOR + id)) {
                writer.write(message);
            } catch (IOException e){
                e.printStackTrace();
            }
        }

        private Message pull() {
            lock(lockFile);
            Message message = null;
            try {
                File[] messageFiles = consumed.listFiles();
                if ( messageFiles != null && messageFiles.length > 0) {
                    Arrays.sort(messageFiles, Comparator.comparingLong(File::lastModified));
                    for(File file : messageFiles) {
                        if (file.lastModified() > System.currentTimeMillis()) {
                            break;
                        } else {
                            message = getMessage(file);
                            updateLastModifiedTime(file);
                            return message;
                        }
                    }
                }

                messageFiles = queueDir.listFiles();
                if(messageFiles != null && messageFiles.length > 0) {
                    Arrays.sort(messageFiles);
                    File file = messageFiles[0];
                    if(file.isFile()) {
                        message = getMessage(file);
                        moveToConsumed(file);
                    }
                }
            } finally {
                unlock(lockFile);
            }
            return message;
        }

        private void delete(String messageId){
            lock(lockFile);
            try {
                File fileToDelete = new File(consumed.getAbsolutePath()+ FILE_SEPARATOR + messageId);
                fileToDelete.delete();
            } finally {
                unlock(lockFile);
            }
        }

        private void moveToConsumed(File file) {
            try {
                File newFile = new File(consumed.getAbsolutePath()+ FILE_SEPARATOR + file.getName());
                Files.move(file.toPath(), newFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
                updateLastModifiedTime(newFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void updateLastModifiedTime(File file){
            try {
                Files.setLastModifiedTime(file.toPath(), FileTime.fromMillis(System.currentTimeMillis() + DEFAULT_VISIBILITY));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private Message getMessage(File file) {
            Message message = null;
            try {

                byte[] contentInBytes = Files.readAllBytes(file.toPath());
                String contentString = new String(contentInBytes);
/*
                List<String> content = Files.readAllLines(file.toPath());
                StringBuilder builder = new StringBuilder();
                int i  = 0;
                for(String msg : content){
                    builder.append(msg);
                    i++;
                    if( i < content.size()) {
                        builder.append(System.lineSeparator());
                    }
                }
                message = new Message(file.getName(), builder.toString());*/

                message = new Message(file.getName(), contentString);
                message.setVisibilityTimeout(System.currentTimeMillis() + DEFAULT_VISIBILITY);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return message;
        }
    }
}