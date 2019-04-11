/*
 * Copyright 2016 - 2019 Acosix GmbH
 */
package de.acosix.alfresco.ignite.repo.cache;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Axel Faust
 */
public class SimpleCacheMetrics implements CacheMetrics
{

    protected final AtomicLong cacheGets = new AtomicLong(0);

    protected final AtomicLong cacheHits = new AtomicLong(0);

    protected final AtomicLong cacheMisses = new AtomicLong(0);

    protected final AtomicLong cacheEvictions = new AtomicLong(0);

    /**
     * {@inheritDoc}
     */
    @Override
    public long getCacheGets()
    {
        return this.cacheGets.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getCacheHits()
    {
        return this.cacheHits.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getCacheHitPercentage()
    {
        final long cacheHits = this.getCacheHits();
        final long cacheGets = this.getCacheGets();
        final double percentage = cacheGets > 0 ? ((cacheHits * 100d) / cacheGets) : 100;
        return percentage;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getCacheMisses()
    {
        return this.cacheMisses.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getCacheMissPercentage()
    {
        final long cacheMisses = this.getCacheMisses();
        final long cacheGets = this.getCacheGets();
        final double percentage = cacheGets > 0 ? ((cacheMisses * 100d) / cacheGets) : 0;
        return percentage;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getCacheEvictions()
    {
        return this.cacheEvictions.get();
    }

    protected void recordHit()
    {
        this.cacheGets.incrementAndGet();
        this.cacheHits.incrementAndGet();
    }

    protected void recordMiss()
    {
        this.cacheGets.incrementAndGet();
        this.cacheMisses.incrementAndGet();
    }

    protected void recordEviction()
    {
        this.cacheEvictions.incrementAndGet();
    }
}
