package com.gunten.batch.batch;

import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.logging.Logger;
import org.mybatis.logging.LoggerFactory;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;

import java.util.List;
import java.util.stream.Collectors;

import static org.springframework.util.Assert.isTrue;
import static org.springframework.util.Assert.notNull;

/**
 * BatchMybatisItemWriter
 *
 * @author Guten-Tag Opensource
 * @since 2024-09-20 11:24:59
 */
public class BatchMybatisItemWriter<T> implements ItemWriter<T>, InitializingBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchMybatisItemWriter.class);

    private SqlSessionTemplate sqlSessionTemplate;

    private String statementId;

    private boolean assertUpdates = true;

    private Converter<T, ?> itemToParameterConverter = new PassThroughConverter<>();

    public void setAssertUpdates(boolean assertUpdates) {
        this.assertUpdates = assertUpdates;
    }

    public void setSqlSessionFactory(SqlSessionFactory sqlSessionFactory) {
        if (sqlSessionTemplate == null) {
            this.sqlSessionTemplate = new SqlSessionTemplate(sqlSessionFactory, ExecutorType.BATCH);
        }
    }

    public void setSqlSessionTemplate(SqlSessionTemplate sqlSessionTemplate) {
        this.sqlSessionTemplate = sqlSessionTemplate;
    }

    public void setStatementId(String statementId) {
        this.statementId = statementId;
    }

    public void setItemToParameterConverter(Converter<T, ?> itemToParameterConverter) {
        this.itemToParameterConverter = itemToParameterConverter;
    }

    @Override
    public void afterPropertiesSet() {
        notNull(sqlSessionTemplate, "A SqlSessionFactory or a SqlSessionTemplate is required.");
        isTrue(ExecutorType.BATCH == sqlSessionTemplate.getExecutorType(),
                "SqlSessionTemplate's executor type must be BATCH");
        notNull(statementId, "A statementId is required.");
        notNull(itemToParameterConverter, "A itemToParameterConverter is required.");
    }

    @Override
    public void write(final List<? extends T> items) {

        if (!items.isEmpty()) {
            LOGGER.debug(() -> "Executing batch with " + items.size() + " items.");

            List<?> convertItemList = items.stream().map(
                    item -> itemToParameterConverter.convert(item)
            ).collect(Collectors.toList());

            sqlSessionTemplate.update(statementId, convertItemList);

            List<BatchResult> results = sqlSessionTemplate.flushStatements();

            if (assertUpdates) {
                if (results.size() != 1) {
                    throw new InvalidDataAccessResourceUsageException("Batch execution returned invalid results. "
                            + "Expected 1 but number of BatchResult objects returned was " + results.size());
                }

                int[] updateCounts = results.get(0).getUpdateCounts();

                for (int i = 0; i < updateCounts.length; i++) {
                    int value = updateCounts[i];
                    if (value == 0) {
                        throw new EmptyResultDataAccessException(
                                "Item " + i + " of " + updateCounts.length + " did not update any rows: [" + items.get(i) + "]", 1);
                    }
                }
            }
        }
    }

    private static class PassThroughConverter<T> implements Converter<T, T> {

        @Override
        public T convert(T source) {
            return source;
        }

    }

}
