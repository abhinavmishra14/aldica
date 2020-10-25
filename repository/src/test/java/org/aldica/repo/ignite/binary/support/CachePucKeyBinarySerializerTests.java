/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
package org.aldica.repo.ignite.binary.support;

import java.lang.reflect.Constructor;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.aldica.common.ignite.GridTestsBase;
import org.aldica.repo.ignite.ExpensiveTestCategory;
import org.alfresco.repo.domain.propval.AbstractPropertyValueDAOImpl.CachePucKey;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Axel Faust
 */
public class CachePucKeyBinarySerializerTests extends GridTestsBase
{

    private static final Logger LOGGER = LoggerFactory.getLogger(CachePucKeyBinarySerializerTests.class);

    protected static IgniteConfiguration createConfiguration(final boolean serialForm, final String... regionNames)
    {
        final IgniteConfiguration conf = createConfiguration(1, false, null);

        final BinaryConfiguration binaryConfiguration = new BinaryConfiguration();

        final BinaryTypeConfiguration binaryTypeConfigurationForCachePucKey = new BinaryTypeConfiguration();
        binaryTypeConfigurationForCachePucKey.setTypeName(CachePucKey.class.getName());
        final CachePucKeyBinarySerializer serializer = new CachePucKeyBinarySerializer();
        serializer.setUseRawSerialForm(serialForm);
        serializer.setUseVariableLengthIntegers(serialForm);
        binaryTypeConfigurationForCachePucKey.setSerializer(serializer);

        binaryConfiguration.setTypeConfigurations(Arrays.asList(binaryTypeConfigurationForCachePucKey));
        conf.setBinaryConfiguration(binaryConfiguration);

        final DataStorageConfiguration dataConf = new DataStorageConfiguration();
        final List<DataRegionConfiguration> regionConfs = new ArrayList<>();
        for (final String regionName : regionNames)
        {
            final DataRegionConfiguration regionConf = new DataRegionConfiguration();
            regionConf.setName(regionName);
            // all regions are 10-100 MiB
            regionConf.setInitialSize(10 * 1024 * 1024);
            regionConf.setMaxSize(100 * 1024 * 1024);
            regionConf.setPageEvictionMode(DataPageEvictionMode.RANDOM_2_LRU);
            regionConf.setMetricsEnabled(true);
            regionConfs.add(regionConf);
        }
        dataConf.setDataRegionConfigurations(regionConfs.toArray(new DataRegionConfiguration[0]));
        conf.setDataStorageConfiguration(dataConf);

        return conf;
    }

    @Test
    public void defaultFormCorrectness() throws Exception
    {
        final IgniteConfiguration conf = createConfiguration(false);
        this.correctnessImpl(conf);
    }

    @Category(ExpensiveTestCategory.class)
    @Test
    public void defaultFormEfficiency() throws Exception
    {
        final IgniteConfiguration referenceConf = createConfiguration(1, false, null);
        referenceConf.setIgniteInstanceName(referenceConf.getIgniteInstanceName() + "-reference");
        final IgniteConfiguration conf = createConfiguration(false, "values");

        referenceConf.setDataStorageConfiguration(conf.getDataStorageConfiguration());

        try
        {
            final Ignite referenceGrid = Ignition.start(referenceConf);
            final Ignite grid = Ignition.start(conf);

            final CacheConfiguration<Long, CachePucKey> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("values");
            cacheConfig.setDataRegionName("values");
            final IgniteCache<Long, CachePucKey> referenceCache = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<Long, CachePucKey> cache = grid.getOrCreateCache(cacheConfig);

            // slightly better performance due to exclusion of hashCode (reduced partially by extra flag) - 2%
            this.efficiencyImpl(referenceGrid, grid, referenceCache, cache, "aldica optimised", "Ignite default", 0.02);
        }
        finally
        {
            Ignition.stopAll(true);
        }
    }

    @Test
    public void rawSerialFormCorrectness() throws Exception
    {
        final IgniteConfiguration conf = createConfiguration(true);
        this.correctnessImpl(conf);
    }

    @Category(ExpensiveTestCategory.class)
    @Test
    public void rawSerialFormEfficiency() throws Exception
    {
        final IgniteConfiguration referenceConf = createConfiguration(false, "values");
        referenceConf.setIgniteInstanceName(referenceConf.getIgniteInstanceName() + "-reference");
        final IgniteConfiguration conf = createConfiguration(true, "values");

        try
        {
            final Ignite referenceGrid = Ignition.start(referenceConf);
            final Ignite grid = Ignition.start(conf);

            final CacheConfiguration<Long, CachePucKey> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("values");
            cacheConfig.setDataRegionName("values");
            final IgniteCache<Long, CachePucKey> referenceCache1 = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<Long, CachePucKey> cache1 = grid.getOrCreateCache(cacheConfig);

            // variable length integers provide significant advantages for all but maximum key value range - 15%
            this.efficiencyImpl(referenceGrid, grid, referenceCache1, cache1, "aldica raw serial", "aldica optimised", 0.15);
        }
        finally
        {
            Ignition.stopAll(true);
        }
    }

    protected void correctnessImpl(final IgniteConfiguration conf) throws Exception
    {
        try (Ignite grid = Ignition.start(conf))
        {
            final CacheConfiguration<Long, CachePucKey> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setName("nodeVersionKey");
            cacheConfig.setCacheMode(CacheMode.LOCAL);
            final IgniteCache<Long, CachePucKey> cache = grid.getOrCreateCache(cacheConfig);

            CachePucKey controlValue;
            CachePucKey cacheValue;

            // CachePucKey is public but does not have public constructor
            final Constructor<CachePucKey> ctor = CachePucKey.class.getDeclaredConstructor(Long.class, Long.class, Long.class);
            ctor.setAccessible(true);

            controlValue = ctor.newInstance(Long.valueOf(1l), Long.valueOf(2l), Long.valueOf(3l));
            cache.put(1l, controlValue);

            cacheValue = cache.get(1l);

            Assert.assertEquals(controlValue, cacheValue);
            // check deep serialisation was actually involved
            Assert.assertNotSame(controlValue, cacheValue);

            controlValue = ctor.newInstance(Long.valueOf(4l), Long.valueOf(5l), null);
            cache.put(2l, controlValue);

            cacheValue = cache.get(2l);

            Assert.assertEquals(controlValue, cacheValue);
            // check deep serialisation was actually involved
            Assert.assertNotSame(controlValue, cacheValue);

            controlValue = ctor.newInstance(Long.valueOf(6l), null, null);
            cache.put(3l, controlValue);

            cacheValue = cache.get(3l);

            Assert.assertEquals(controlValue, cacheValue);
            // check deep serialisation was actually involved
            Assert.assertNotSame(controlValue, cacheValue);

            controlValue = ctor.newInstance(null, null, null);
            cache.put(4l, controlValue);

            cacheValue = cache.get(4l);

            Assert.assertEquals(controlValue, cacheValue);
            // check deep serialisation was actually involved
            Assert.assertNotSame(controlValue, cacheValue);
        }
    }

    protected void efficiencyImpl(final Ignite referenceGrid, final Ignite grid, final IgniteCache<Long, CachePucKey> referenceCache,
            final IgniteCache<Long, CachePucKey> cache, final String serialisationType, final String referenceSerialisationType,
            final double marginFraction) throws Exception
    {
        LOGGER.info(
                "Running CachePucKey serialisation benchmark of 100k instances, comparing {} vs. {} serialisation, expecting relative improvement margin / difference fraction of {}",
                referenceSerialisationType, serialisationType, marginFraction);

        // CachePucKey is public but does not have public constructor
        final Constructor<CachePucKey> ctor = CachePucKey.class.getDeclaredConstructor(Long.class, Long.class, Long.class);
        ctor.setAccessible(true);

        final SecureRandom rnJesus = new SecureRandom();
        for (int idx = 0; idx < 100000; idx++)
        {
            final long id = idx;
            // due to deduplication, majority of (often used) values in alf_prop_unique_ctx would be quite low
            final CachePucKey value = ctor.newInstance(Long.valueOf(rnJesus.nextInt(10000000)), Long.valueOf(rnJesus.nextInt(10000000)),
                    Long.valueOf(rnJesus.nextInt(10000000)));
            referenceCache.put(id, value);
            cache.put(id, value);
        }

        @SuppressWarnings("unchecked")
        final String regionName = cache.getConfiguration(CacheConfiguration.class).getDataRegionName();
        final DataRegionMetrics referenceMetrics = referenceGrid.dataRegionMetrics(regionName);
        final DataRegionMetrics metrics = grid.dataRegionMetrics(regionName);

        // sufficient to compare used pages - byte-exact memory usage cannot be determined due to potential partial page fill
        final long referenceTotalUsedPages = referenceMetrics.getTotalUsedPages();
        final long totalUsedPages = metrics.getTotalUsedPages();
        final long allowedMax = referenceTotalUsedPages - (long) (marginFraction * referenceTotalUsedPages);
        LOGGER.info("Benchmark resulted in {} vs {} (expected max of {}) total used pages", referenceTotalUsedPages, totalUsedPages,
                allowedMax);
        Assert.assertTrue(totalUsedPages <= allowedMax);
    }
}
