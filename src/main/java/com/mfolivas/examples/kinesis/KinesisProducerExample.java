package com.mfolivas.examples.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Kinesis example.
 * 24478904
 */
public class KinesisProducerExample {
    public static void main(String[] args) {
        
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        
        AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        AmazonKinesisClient amazonKinesisClient = new AmazonKinesisClient(credentialsProvider);
        PutRecordsRequest putRecordsRequest  = new PutRecordsRequest();
        putRecordsRequest.setStreamName("sfw-midas");
        List<PutRecordsRequestEntry> putRecordsRequestEntryList  = new ArrayList();
        for (int i = 0; i < 5; i++) {
            Date createTime = new Date();
            PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry()
                    .withData(ByteBuffer.wrap(String.format("%d-ThirdTestData-%tFT%<tT.%<tL%<tz", i, createTime).getBytes()))
                    .withPartitionKey(String.format("partitionKey-%d", i))
                    .withExplicitHashKey(String.format("%d", i));
            
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }

        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        PutRecordsResult putRecordsResult  = amazonKinesisClient.putRecords(putRecordsRequest);
        
        int count =0;
        System.out.println("Result FailedRecordCount:" + putRecordsResult.getFailedRecordCount());
        
        for (PutRecordsResultEntry putRecordsResultEntry : putRecordsResult.getRecords()) {
            System.out.format("%d - Record: %s\n" ,++count, putRecordsResultEntry);
        } 
        
    }
}
