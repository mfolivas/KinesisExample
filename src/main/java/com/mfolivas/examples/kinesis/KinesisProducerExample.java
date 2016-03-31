package com.mfolivas.examples.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Kinesis example.
 * 24478904
 */
public class KinesisProducerExample {
    public static void main(String[] args) {
        AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        AmazonKinesisClient amazonKinesisClient = new AmazonKinesisClient(credentialsProvider);
        PutRecordsRequest putRecordsRequest  = new PutRecordsRequest();
        putRecordsRequest.setStreamName("test-stream");
        List<PutRecordsRequestEntry> putRecordsRequestEntryList  = new ArrayList();
        for (int i = 0; i < 100; i++) {
            long createTime = System.currentTimeMillis();
            PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(ByteBuffer.wrap(String.format("testData-%d", createTime).getBytes()));
            putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", i));
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }

        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        PutRecordsResult putRecordsResult  = amazonKinesisClient.putRecords(putRecordsRequest);
        System.out.println("Put Result" + putRecordsResult);
    }
}
