package com.buvi.queue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;

class FileQueueTest extends QueueTest {

    @BeforeAll
    private static void clearQueues(){
        File file = new File(System.getProperty("user.home")+"/queue-messages/");
        File[] queues = file.listFiles();
        if(queues != null){
            for(File queue : queues){
                deleteDirectory(queue);
            }
        }
    }

    @BeforeEach
    private void setUp() {
        queueService = QueueFactory.getFileQueue();
    }

    @AfterEach
    private void tearDown() {
    }

    private static void deleteDirectory(File directory){
        File[] files = directory.listFiles();
        if(files != null){
            for(File file : files){
                if(file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
    }
}