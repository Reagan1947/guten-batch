package com.gunten.batch.batch;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

/**
 * JobListener
 *
 * @author Guten-Tag Opensource
 * @since 2024-09-20 11:24:59
 */
public class JobListener implements JobExecutionListener {


    @Override
    public void beforeJob(JobExecution jobExecution) {
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
    }
}