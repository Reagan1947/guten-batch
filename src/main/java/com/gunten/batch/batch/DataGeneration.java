package com.gunten.batch.batch;

/**
 * DataGeneration
 *
 * @author Guten-Tag Opensource
 * @since 2024-09-20 11:24:59
 */
public abstract class DataGeneration<T> {

    public abstract T generate(Integer currentIndex);
 }
