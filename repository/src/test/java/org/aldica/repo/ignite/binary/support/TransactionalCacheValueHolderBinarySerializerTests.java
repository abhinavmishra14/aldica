/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
package org.aldica.repo.ignite.binary.support;

import java.io.Serializable;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

import org.aldica.common.ignite.GridTestsBase;
import org.aldica.repo.ignite.ExpensiveTestCategory;
import org.aldica.repo.ignite.binary.base.AbstractCustomBinarySerializer;
import org.aldica.repo.ignite.binary.support.TransactionalCacheValueHolderBinarySerializer;
import org.aldica.repo.ignite.cache.SimpleIgniteBackedCache;
import org.aldica.repo.ignite.cache.SimpleIgniteBackedCache.Mode;
import org.alfresco.repo.cache.TransactionalCache;
import org.alfresco.repo.cache.TransactionalCache.ValueHolder;
import org.alfresco.service.cmr.repository.StoreRef;
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
public class TransactionalCacheValueHolderBinarySerializerTests extends GridTestsBase
{

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalCacheValueHolderBinarySerializerTests.class);

    protected static IgniteConfiguration createConfiguration(final boolean serialForm, final String... regionNames)
    {
        final IgniteConfiguration conf = createConfiguration(1, false, null);

        final BinaryConfiguration binaryConfiguration = new BinaryConfiguration();

        final BinaryTypeConfiguration binaryTypeConfigurationForQName = new BinaryTypeConfiguration();
        binaryTypeConfigurationForQName.setTypeName(ValueHolder.class.getName());
        final TransactionalCacheValueHolderBinarySerializer serializer = new TransactionalCacheValueHolderBinarySerializer();
        serializer.setUseRawSerialForm(serialForm);
        serializer.setUseVariableLengthIntegers(serialForm);
        binaryTypeConfigurationForQName.setSerializer(serializer);

        binaryConfiguration.setTypeConfigurations(Arrays.asList(binaryTypeConfigurationForQName));
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
    public void defaultFormCorrectness()
    {
        final IgniteConfiguration conf = createConfiguration(false);
        this.correctnessImpl(conf);
    }

    @Category(ExpensiveTestCategory.class)
    @Test
    public void defaultFormEfficiency()
    {
        final IgniteConfiguration referenceConf = createConfiguration(1, false, null);
        referenceConf.setIgniteInstanceName(referenceConf.getIgniteInstanceName() + "-reference");
        final IgniteConfiguration conf = createConfiguration(false, "values");

        referenceConf.setDataStorageConfiguration(conf.getDataStorageConfiguration());

        try
        {
            final Ignite referenceGrid = Ignition.start(referenceConf);
            final Ignite grid = Ignition.start(conf);

            final CacheConfiguration<Long, ValueHolder<Serializable>> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("values");
            cacheConfig.setDataRegionName("values");
            final IgniteCache<Long, ValueHolder<Serializable>> referenceCache = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<Long, ValueHolder<Serializable>> cache = grid.getOrCreateCache(cacheConfig);

            // depending on values, default optimised form ends up slightly more efficient to equal or narrowly less efficient
            // small "added overhead" is the single byte for the type flag, which drives internal optimisations
            // strings / lists / sets will be narrowly less efficient due to the type flag byte
            // sentinel values for "not found" / "null" will be significantly more efficient as value is already encoded in the flag byte
            // dates are slightly more efficient as they are inlined

            // overall (with value distribution in efficiencyImpl) default optimised form is slightly more efficient - ~2%
            this.efficiencyImpl(referenceGrid, grid, referenceCache, cache, "aldica optimised", "Ignite default", 0.02);
        }
        finally
        {
            Ignition.stopAll(true);
        }
    }

    @Test
    public void defaultIgniteClassHandlingCorrectness()
    {
        final IgniteConfiguration defaultConf = createConfiguration(1, false, null);
        defaultConf.setIgniteInstanceName(defaultConf.getIgniteInstanceName() + "-default");

        final Ignite defaultGrid = Ignition.start(defaultConf);
        try
        {
            final CacheConfiguration<Long, Class<?>> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);
            cacheConfig.setName("values");

            final IgniteCache<Long, Class<?>> defaultCache = defaultGrid.getOrCreateCache(cacheConfig);

            Class<?> controlValue;
            Class<?> cacheValue;

            controlValue = HashMap.class;
            defaultCache.put(1l, controlValue);
            cacheValue = defaultCache.get(1l);

            Assert.assertEquals(controlValue, cacheValue);

            controlValue = ValueHolder.class;
            defaultCache.put(2l, controlValue);
            cacheValue = defaultCache.get(2l);

            Assert.assertEquals(controlValue, cacheValue);
        }
        finally
        {
            Ignition.stopAll(true);
        }
    }

    @Test
    public void rawSerialFormCorrectness()
    {
        final IgniteConfiguration conf = createConfiguration(true);
        this.correctnessImpl(conf);
    }

    @Category(ExpensiveTestCategory.class)
    @Test
    public void rawSerialFormEfficiency()
    {
        final IgniteConfiguration referenceConf = createConfiguration(false, "values");
        referenceConf.setIgniteInstanceName(referenceConf.getIgniteInstanceName() + "-reference");
        final IgniteConfiguration conf = createConfiguration(true, "values");

        try
        {
            final Ignite referenceGrid = Ignition.start(referenceConf);
            final Ignite grid = Ignition.start(conf);

            final CacheConfiguration<Long, ValueHolder<Serializable>> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("values");
            cacheConfig.setDataRegionName("values");
            final IgniteCache<Long, ValueHolder<Serializable>> referenceCache1 = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<Long, ValueHolder<Serializable>> cache1 = grid.getOrCreateCache(cacheConfig);

            // variable length integers for dates + size/length meta fields saves some memory - 7%
            this.efficiencyImpl(referenceGrid, grid, referenceCache1, cache1, "aldica raw serial", "aldica optimised", 0.07);
        }
        finally
        {
            Ignition.stopAll(true);
        }
    }

    protected void correctnessImpl(final IgniteConfiguration conf)
    {
        try (Ignite grid = Ignition.start(conf))
        {
            final CacheConfiguration<Long, ValueHolder<Serializable>> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setName("valueHolder");
            cacheConfig.setCacheMode(CacheMode.LOCAL);
            final IgniteCache<Long, ValueHolder<Serializable>> cache = grid.getOrCreateCache(cacheConfig);
            // need to allow sentinels in order to verify them as values
            final SimpleIgniteBackedCache<Long, ValueHolder<Serializable>> simpleCache = new SimpleIgniteBackedCache<>(grid, Mode.LOCAL,
                    cache, true);

            Serializable controlValue;
            Serializable cacheValue;

            controlValue = TransactionalCacheValueHolderBinarySerializer.ENTITY_LOOKUP_CACHE_NOT_FOUND;
            TransactionalCache.putSharedCacheValue(simpleCache, Long.valueOf(1L), controlValue, null);

            cacheValue = TransactionalCache.getSharedCacheValue(simpleCache, Long.valueOf(1L));

            Assert.assertEquals(controlValue, cacheValue);
            // in this case, no deep cloning should be taking place
            Assert.assertTrue(controlValue == cacheValue);

            controlValue = TransactionalCacheValueHolderBinarySerializer.ENTITY_LOOKUP_CACHE_NULL;
            TransactionalCache.putSharedCacheValue(simpleCache, Long.valueOf(2L), controlValue, null);

            cacheValue = TransactionalCache.getSharedCacheValue(simpleCache, Long.valueOf(2L));

            Assert.assertEquals(controlValue, cacheValue);
            // in this case, no deep cloning should be taking place
            Assert.assertTrue(controlValue == cacheValue);

            controlValue = new Date(AbstractCustomBinarySerializer.LONG_AS_SHORT_SIGNED_POSITIVE_MAX + 100);
            TransactionalCache.putSharedCacheValue(simpleCache, Long.valueOf(3L), controlValue, null);

            cacheValue = TransactionalCache.getSharedCacheValue(simpleCache, Long.valueOf(3L));

            Assert.assertEquals(controlValue, cacheValue);
            Assert.assertFalse(controlValue == cacheValue);

            controlValue = new Date(AbstractCustomBinarySerializer.LONG_AS_SHORT_SIGNED_POSITIVE_MAX - 100);
            TransactionalCache.putSharedCacheValue(simpleCache, Long.valueOf(4L), controlValue, null);

            cacheValue = TransactionalCache.getSharedCacheValue(simpleCache, Long.valueOf(4L));

            Assert.assertEquals(controlValue, cacheValue);
            Assert.assertFalse(controlValue == cacheValue);

            controlValue = new ArrayList<>(Arrays.asList("Test1", "Test2", "Test3", "Test4"));
            TransactionalCache.putSharedCacheValue(simpleCache, Long.valueOf(5L), controlValue, null);

            cacheValue = TransactionalCache.getSharedCacheValue(simpleCache, Long.valueOf(5L));

            Assert.assertEquals(controlValue, cacheValue);
            Assert.assertFalse(controlValue == cacheValue);
            Assert.assertEquals(controlValue.getClass(), cacheValue.getClass());

            controlValue = new LinkedList<>(Arrays.asList("Test1", "Test2", "Test3", "Test4"));
            TransactionalCache.putSharedCacheValue(simpleCache, Long.valueOf(6L), controlValue, null);

            cacheValue = TransactionalCache.getSharedCacheValue(simpleCache, Long.valueOf(6L));

            Assert.assertEquals(controlValue, cacheValue);
            Assert.assertFalse(controlValue == cacheValue);
            Assert.assertNotEquals(controlValue.getClass(), cacheValue.getClass());
            Assert.assertEquals(ArrayList.class, cacheValue.getClass());

            controlValue = new ArrayList<>(Arrays.asList(Long.valueOf(1), new Date()));
            TransactionalCache.putSharedCacheValue(simpleCache, Long.valueOf(7L), controlValue, null);

            cacheValue = TransactionalCache.getSharedCacheValue(simpleCache, Long.valueOf(7L));

            Assert.assertEquals(controlValue, cacheValue);
            Assert.assertFalse(controlValue == cacheValue);
            Assert.assertEquals(controlValue.getClass(), cacheValue.getClass());

            controlValue = new LinkedList<>(Arrays.asList(Long.valueOf(1), new Date()));
            TransactionalCache.putSharedCacheValue(simpleCache, Long.valueOf(8L), controlValue, null);

            cacheValue = TransactionalCache.getSharedCacheValue(simpleCache, Long.valueOf(8L));

            Assert.assertEquals(controlValue, cacheValue);
            Assert.assertFalse(controlValue == cacheValue);
            Assert.assertNotEquals(controlValue.getClass(), cacheValue.getClass());
            Assert.assertEquals(ArrayList.class, cacheValue.getClass());

            controlValue = new HashSet<>(Arrays.asList("Test1", "Test2", "Test3", "Test4"));
            TransactionalCache.putSharedCacheValue(simpleCache, Long.valueOf(9L), controlValue, null);

            cacheValue = TransactionalCache.getSharedCacheValue(simpleCache, Long.valueOf(9L));

            Assert.assertEquals(controlValue, cacheValue);
            Assert.assertFalse(controlValue == cacheValue);
            Assert.assertEquals(controlValue.getClass(), cacheValue.getClass());

            controlValue = new LinkedHashSet<>(Arrays.asList("Test1", "Test2", "Test3", "Test4"));
            TransactionalCache.putSharedCacheValue(simpleCache, Long.valueOf(10L), controlValue, null);

            cacheValue = TransactionalCache.getSharedCacheValue(simpleCache, Long.valueOf(10L));

            Assert.assertEquals(controlValue, cacheValue);
            Assert.assertFalse(controlValue == cacheValue);
            Assert.assertNotEquals(controlValue.getClass(), cacheValue.getClass());
            Assert.assertEquals(HashSet.class, cacheValue.getClass());

            controlValue = new HashSet<>(Arrays.asList(Long.valueOf(1), new Date()));
            TransactionalCache.putSharedCacheValue(simpleCache, Long.valueOf(11L), controlValue, null);

            cacheValue = TransactionalCache.getSharedCacheValue(simpleCache, Long.valueOf(11L));

            Assert.assertEquals(controlValue, cacheValue);
            Assert.assertFalse(controlValue == cacheValue);
            Assert.assertEquals(controlValue.getClass(), cacheValue.getClass());

            controlValue = new LinkedHashSet<>(Arrays.asList(Long.valueOf(1), new Date()));
            TransactionalCache.putSharedCacheValue(simpleCache, Long.valueOf(12L), controlValue, null);

            cacheValue = TransactionalCache.getSharedCacheValue(simpleCache, Long.valueOf(12L));

            Assert.assertEquals(controlValue, cacheValue);
            Assert.assertFalse(controlValue == cacheValue);
            Assert.assertNotEquals(controlValue.getClass(), cacheValue.getClass());
            Assert.assertEquals(HashSet.class, cacheValue.getClass());

            controlValue = "Test123456798";
            TransactionalCache.putSharedCacheValue(simpleCache, Long.valueOf(13L), controlValue, null);

            cacheValue = TransactionalCache.getSharedCacheValue(simpleCache, Long.valueOf(13L));

            Assert.assertEquals(controlValue, cacheValue);
            Assert.assertFalse(controlValue == cacheValue);

            controlValue = StoreRef.STORE_REF_WORKSPACE_SPACESSTORE;
            TransactionalCache.putSharedCacheValue(simpleCache, Long.valueOf(14L), controlValue, null);

            cacheValue = TransactionalCache.getSharedCacheValue(simpleCache, Long.valueOf(14L));

            Assert.assertEquals(controlValue, cacheValue);
            Assert.assertFalse(controlValue == cacheValue);
        }
    }

    protected void efficiencyImpl(final Ignite referenceGrid, final Ignite grid,
            final IgniteCache<Long, ValueHolder<Serializable>> referenceCache, final IgniteCache<Long, ValueHolder<Serializable>> cache,
            final String serialisationType, final String referenceSerialisationType, final double marginFraction)
    {
        LOGGER.info(
                "Running ValueHolder serialisation benchmark of 100k instances, comparing {} vs. {} serialisation, expecting relative improvement margin / difference fraction of {}",
                referenceSerialisationType, serialisationType, marginFraction);

        // need to allow sentinels in order to verify them as values
        final SimpleIgniteBackedCache<Long, ValueHolder<Serializable>> simpleCache = new SimpleIgniteBackedCache<>(grid, Mode.LOCAL, cache,
                true);
        final SimpleIgniteBackedCache<Long, ValueHolder<Serializable>> simpleReferenceCache = new SimpleIgniteBackedCache<>(referenceGrid,
                Mode.LOCAL, referenceCache, true);

        final SecureRandom rnJesus = new SecureRandom();

        // in this test we only vary on value types, but use same values for each type to keep fluctuation of sizes low
        for (int idx = 0; idx < 100000; idx++)
        {
            final double chanceSelector = rnJesus.nextDouble();
            final int type;

            // 15% each for sentinel values - these are the most prolific in real life as well
            if (chanceSelector < .15)
            {
                type = 0;
            }
            else if (chanceSelector < .3)
            {
                type = 1;
            }
            else
            {
                // equal chances for rest (we do not include mixed value collections as these are not used in real life uses)
                type = 2 + rnJesus.nextInt(8);
            }

            Serializable value = null;

            switch (type)
            {
                case 0:
                    value = TransactionalCacheValueHolderBinarySerializer.ENTITY_LOOKUP_CACHE_NOT_FOUND;
                    break;
                case 1:
                    value = TransactionalCacheValueHolderBinarySerializer.ENTITY_LOOKUP_CACHE_NULL;
                    break;
                case 2:
                    value = StoreRef.STORE_REF_WORKSPACE_SPACESSTORE;
                    break;
                case 3:
                    value = "test1234";
                    break;
                case 4:
                    value = new Date(AbstractCustomBinarySerializer.LONG_AS_SHORT_SIGNED_POSITIVE_MAX + 100);
                    break;
                case 5:
                    value = new Date(AbstractCustomBinarySerializer.LONG_AS_SHORT_SIGNED_POSITIVE_MAX - 100);
                    break;
                case 6:
                    value = new ArrayList<>(Arrays.asList("test1234", "test5678"));
                    break;
                case 7:
                    value = new LinkedList<>(Arrays.asList("test1234", "test5678"));
                    break;
                case 8:
                    value = new HashSet<>(Arrays.asList("test1234", "test5678"));
                    break;
                case 9:
                    value = new LinkedHashSet<>(Arrays.asList("test1234", "test5678"));
                    break;
            }

            TransactionalCache.putSharedCacheValue(simpleCache, Long.valueOf(idx), value, null);
            TransactionalCache.putSharedCacheValue(simpleReferenceCache, Long.valueOf(idx), value, null);
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
