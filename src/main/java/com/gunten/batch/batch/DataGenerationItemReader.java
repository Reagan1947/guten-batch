package com.gunten.batch.batch;

import org.springframework.batch.item.*;

/**
 * DataGenerationItemReader
 *
 * @author Guten-Tag Opensource
 * @since 2024-09-20 11:24:59
 */
public class DataGenerationItemReader<T> implements ItemReader<T>, ItemStream {

    DataGeneration<T> dataGeneration;
    Integer generationDataSize;

    int currentIndex = 0;
    private static final String CURRENT_INDEX = "current.index";

    public DataGenerationItemReader(Integer generationDataSize, DataGeneration<T> dataGeneration) {
        this.generationDataSize = generationDataSize;
        this.dataGeneration = dataGeneration;
    }

    public T read() throws ParseException, NonTransientResourceException {

        if (currentIndex < generationDataSize) {
            currentIndex += 1;
            return dataGeneration.generate(currentIndex);
        }

        return null;
    }

    public void open(ExecutionContext executionContext) throws ItemStreamException {
        if (executionContext.containsKey(CURRENT_INDEX)) {
            currentIndex = new Long(executionContext.getLong(CURRENT_INDEX)).intValue();
        }
        else {
            currentIndex = 0;
        }
    }

    public void update(ExecutionContext executionContext) throws ItemStreamException {
        executionContext.putLong(CURRENT_INDEX, currentIndex);
    }

    public void close() throws ItemStreamException {}
}
