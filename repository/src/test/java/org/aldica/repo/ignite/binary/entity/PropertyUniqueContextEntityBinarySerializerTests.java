/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
package org.aldica.repo.ignite.binary.entity;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.aldica.common.ignite.GridTestsBase;
import org.aldica.repo.ignite.ExpensiveTestCategory;
import org.alfresco.repo.domain.propval.PropertyUniqueContextEntity;
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
public class PropertyUniqueContextEntityBinarySerializerTests extends GridTestsBase
{

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertyUniqueContextEntityBinarySerializerTests.class);

    protected static IgniteConfiguration createConfiguration(final boolean serialForm, final String... regionNames)
    {
        final IgniteConfiguration conf = createConfiguration(1, false, null);

        final BinaryConfiguration binaryConfiguration = new BinaryConfiguration();

        final BinaryTypeConfiguration binaryTypeConfigurationForPropertyUniqueContextEntity = new BinaryTypeConfiguration();
        binaryTypeConfigurationForPropertyUniqueContextEntity.setTypeName(PropertyUniqueContextEntity.class.getName());
        final PropertyUniqueContextEntityBinarySerializer serializer = new PropertyUniqueContextEntityBinarySerializer();
        serializer.setUseRawSerialForm(serialForm);
        serializer.setUseVariableLengthIntegers(serialForm);
        binaryTypeConfigurationForPropertyUniqueContextEntity.setSerializer(serializer);

        binaryConfiguration.setTypeConfigurations(Arrays.asList(binaryTypeConfigurationForPropertyUniqueContextEntity));
        conf.setBinaryConfiguration(binaryConfiguration);

        final DataStorageConfiguration dataConf = new DataStorageConfiguration();
        // we have some large-ish value objects
        dataConf.setPageSize(8 * 1024);
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
        final IgniteConfiguration conf = createConfiguration(false, "comparison");

        referenceConf.setDataStorageConfiguration(conf.getDataStorageConfiguration());

        try
        {
            final Ignite referenceGrid = Ignition.start(referenceConf);
            final Ignite grid = Ignition.start(conf);

            final CacheConfiguration<Long, PropertyUniqueContextEntity> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("pucs");
            cacheConfig.setDataRegionName("comparison");
            final IgniteCache<Long, PropertyUniqueContextEntity> referenceCache = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<Long, PropertyUniqueContextEntity> cache = grid.getOrCreateCache(cacheConfig);

            // no real advantage (minor disadvantage even due to additional flag)
            this.efficiencyImpl(referenceGrid, grid, referenceCache, cache, "aldica optimised", "Ignite default", -0.03);
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
        final IgniteConfiguration referenceConf = createConfiguration(false, "comparison");
        referenceConf.setIgniteInstanceName(referenceConf.getIgniteInstanceName() + "-reference");
        final IgniteConfiguration conf = createConfiguration(true, "comparison");

        try
        {
            final Ignite referenceGrid = Ignition.start(referenceConf);
            final Ignite grid = Ignition.start(conf);

            final CacheConfiguration<Long, PropertyUniqueContextEntity> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("pucs");
            cacheConfig.setDataRegionName("comparison");
            final IgniteCache<Long, PropertyUniqueContextEntity> referenceCache = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<Long, PropertyUniqueContextEntity> cache = grid.getOrCreateCache(cacheConfig);

            // variable length integers provide significant advantages for all but maximum key value range - 15%
            this.efficiencyImpl(referenceGrid, grid, referenceCache, cache, "aldica raw serial", "aldica optimised", 0.15);
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
            final CacheConfiguration<Long, PropertyUniqueContextEntity> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setName("stores");
            cacheConfig.setCacheMode(CacheMode.LOCAL);
            final IgniteCache<Long, PropertyUniqueContextEntity> cache = grid.getOrCreateCache(cacheConfig);

            PropertyUniqueContextEntity controlValue;
            PropertyUniqueContextEntity cacheValue;

            controlValue = new PropertyUniqueContextEntity();
            controlValue.setId(1l);
            controlValue.setVersion((short) 1);
            controlValue.setValue1PropId(1l);
            controlValue.setValue2PropId(2l);
            controlValue.setValue3PropId(3l);
            controlValue.setPropertyId(4l);

            cache.put(1l, controlValue);
            cacheValue = cache.get(1l);

            // can't check for equals - value class does not support it
            // check deep serialisation was actually involved (different value instances)
            Assert.assertNotSame(controlValue, cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getVersion(), cacheValue.getVersion());
            Assert.assertEquals(controlValue.getValue1PropId(), cacheValue.getValue1PropId());
            Assert.assertEquals(controlValue.getValue2PropId(), cacheValue.getValue2PropId());
            Assert.assertEquals(controlValue.getValue3PropId(), cacheValue.getValue3PropId());
            Assert.assertEquals(controlValue.getPropertyId(), cacheValue.getPropertyId());

            controlValue = new PropertyUniqueContextEntity();
            controlValue.setId(2l);
            controlValue.setVersion(Short.MAX_VALUE);
            controlValue.setValue1PropId(5l);
            controlValue.setValue2PropId(6l);
            controlValue.setValue3PropId(null);
            controlValue.setPropertyId(7l);

            cache.put(2l, controlValue);
            cacheValue = cache.get(2l);

            // can't check for equals - value class does not support it
            // check deep serialisation was actually involved (different value instances)
            Assert.assertNotSame(controlValue, cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getVersion(), cacheValue.getVersion());
            Assert.assertEquals(controlValue.getValue1PropId(), cacheValue.getValue1PropId());
            Assert.assertEquals(controlValue.getValue2PropId(), cacheValue.getValue2PropId());
            Assert.assertEquals(controlValue.getValue3PropId(), cacheValue.getValue3PropId());
            Assert.assertEquals(controlValue.getPropertyId(), cacheValue.getPropertyId());

            controlValue = new PropertyUniqueContextEntity();
            controlValue.setId(3l);
            controlValue.setVersion(Short.MIN_VALUE);
            controlValue.setValue1PropId(8l);
            controlValue.setValue2PropId(null);
            controlValue.setValue3PropId(null);
            controlValue.setPropertyId(8l);

            cache.put(3l, controlValue);
            cacheValue = cache.get(3l);

            // can't check for equals - value class does not support it
            // check deep serialisation was actually involved (different value instances)
            Assert.assertNotSame(controlValue, cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getVersion(), cacheValue.getVersion());
            Assert.assertEquals(controlValue.getValue1PropId(), cacheValue.getValue1PropId());
            Assert.assertEquals(controlValue.getValue2PropId(), cacheValue.getValue2PropId());
            Assert.assertEquals(controlValue.getValue3PropId(), cacheValue.getValue3PropId());
            Assert.assertEquals(controlValue.getPropertyId(), cacheValue.getPropertyId());
        }
    }

    protected void efficiencyImpl(final Ignite referenceGrid, final Ignite grid,
            final IgniteCache<Long, PropertyUniqueContextEntity> referenceCache, final IgniteCache<Long, PropertyUniqueContextEntity> cache,
            final String serialisationType, final String referenceSerialisationType, final double marginFraction)
    {
        LOGGER.info(
                "Running PropertyUniqueContextEntity serialisation benchmark of 100k instances, comparing {} vs. {} serialisation, expecting relative improvement margin / difference fraction of {}",
                referenceSerialisationType, serialisationType, marginFraction);

        final SecureRandom rnJesus = new SecureRandom();

        for (int idx = 0; idx < 100000; idx++)
        {
            final PropertyUniqueContextEntity value = new PropertyUniqueContextEntity();
            value.setId(Long.valueOf(idx));
            value.setVersion((short) rnJesus.nextInt(Short.MAX_VALUE));
            // due to deduplication, majority of (often used) values in alf_prop_unique_ctx would be quite low
            value.setValue1PropId(Long.valueOf(rnJesus.nextInt(10000000)));
            value.setValue2PropId(Long.valueOf(rnJesus.nextInt(10000000)));
            value.setValue3PropId(Long.valueOf(rnJesus.nextInt(10000000)));
            value.setPropertyId(Long.valueOf(rnJesus.nextInt(10000000)));

            cache.put(value.getId(), value);
            referenceCache.put(value.getId(), value);
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
