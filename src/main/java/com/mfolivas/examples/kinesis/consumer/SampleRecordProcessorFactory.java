package com.mfolivas.examples.kinesis.consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

/**
 * @author Marcelo Olivas
 */
public class SampleRecordProcessorFactory implements IRecordProcessorFactory {
    public IRecordProcessor createProcessor() {
        return new SampleRecordProcessor();
    }
}
