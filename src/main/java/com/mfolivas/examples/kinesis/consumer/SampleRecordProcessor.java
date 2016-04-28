package com.mfolivas.examples.kinesis.consumer;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

/**
 * @author Marcelo Olivas
 */
public class SampleRecordProcessor implements IRecordProcessor {
    // Backoff and retry settings
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 10;

    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

    private InitializationInput initializationInput;

    public void initialize(InitializationInput initializationInput) {
        System.out.format("initializationInput  ShardId [%s], SequenceNumber: %s\n",initializationInput.getShardId(), initializationInput.getExtendedSequenceNumber());
        this.initializationInput = initializationInput;
    }

    public void processRecords(ProcessRecordsInput processRecordsInput) {
        System.out.println("processRecordsInput = [" + processRecordsInput + "]");
        List<Record> records = processRecordsInput.getRecords();
       
        processRecordsWithRetries(records);
        
        try {
            processRecordsInput.getCheckpointer().checkpoint();
        } catch (KinesisClientLibDependencyException | InvalidStateException | ThrottlingException
                | ShutdownException e) {
            // TODO Auto-generated catch block
            e.printStackTrace(System.out);
        }

    }

    /**
     * Process records performing retries as needed. Skip "poison pill" records.
     *
     * @param records Data records to be processed.
     */
    private void processRecordsWithRetries(List<Record> records) {
        records.stream().forEach(record -> processSingleRecord(record));
    }


    private void processSingleRecord(Record record) {
        // TODO Add your own record processing logic here

        String data = null;
        try {
            // For this app, we interpret the payload as UTF-8 chars.
            data = decoder.decode(record.getData()).toString();
            // Assume this record came from AmazonKinesisSample and log its age.
//            long recordCreateTime = new Long(data.substring("testData-".length()));
//            long ageOfRecordInMillis = System.currentTimeMillis() - recordCreateTime;

            System.out.println(record.getSequenceNumber() + ", " + record.getPartitionKey() + ", " + data );
        } catch (NumberFormatException e) {
            System.out.println("Record does not match sample record format. Ignoring record with data; " + data);
        } catch (CharacterCodingException e) {
            System.out.println("Malformed data: " + data + " - " + e);
        }
    }

    public void shutdown(ShutdownInput shutdownInput) {
        System.out.println("shutdownInput = [" + shutdownInput + "]");
    }
}
