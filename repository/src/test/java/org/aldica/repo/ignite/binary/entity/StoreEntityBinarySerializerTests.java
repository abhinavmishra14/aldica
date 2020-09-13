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
import org.alfresco.repo.domain.node.NodeEntity;
import org.alfresco.repo.domain.node.StoreEntity;
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
public class StoreEntityBinarySerializerTests extends GridTestsBase
{

    private static final String PROTOCOL_USER = "user";

    private static final String PROTOCOL_SYSTEM = "system";

    // relevant protocols - no support for legacy avm or any test-like ones (test/deleted) - plus one custom one
    private static final String[] PROTOCOLS = { PROTOCOL_USER, PROTOCOL_SYSTEM, StoreRef.PROTOCOL_ARCHIVE, StoreRef.PROTOCOL_WORKSPACE,
            "custom" };

    private static final String IDENTIFIER_ALFRESCO_USER_STORE = "alfrescoUserStore";

    private static final String IDENTIFIER_SYSTEM = "system";

    private static final String IDENTIFIER_LIGHT_WEIGHT_VERSION_STORE = "lightWeightVersionStore";

    private static final String IDENTIFIER_VERSION_2_STORE = "version2Store";

    private static final String IDENTIFIER_SPACES_STORE = "SpacesStore";

    // relevant default stores - no support for tenant-specific store identifiers - plus one custom one
    private static final String[] IDENTIFIERS = { IDENTIFIER_ALFRESCO_USER_STORE, IDENTIFIER_SYSTEM, IDENTIFIER_LIGHT_WEIGHT_VERSION_STORE,
            IDENTIFIER_VERSION_2_STORE, IDENTIFIER_SPACES_STORE, "myStore" };

    private static final Logger LOGGER = LoggerFactory.getLogger(StoreEntityBinarySerializerTests.class);

    protected static IgniteConfiguration createConfiguration(final boolean serialForm, final String... regionNames)
    {
        final IgniteConfiguration conf = createConfiguration(1, false, null);

        final BinaryConfiguration binaryConfiguration = new BinaryConfiguration();

        final BinaryTypeConfiguration binaryTypeConfigurationForStoreRef = new BinaryTypeConfiguration();
        binaryTypeConfigurationForStoreRef.setTypeName(StoreEntity.class.getName());
        final StoreEntityBinarySerializer serializer = new StoreEntityBinarySerializer();
        serializer.setUseRawSerialForm(serialForm);
        serializer.setUseVariableLengthIntegers(serialForm);
        binaryTypeConfigurationForStoreRef.setSerializer(serializer);

        binaryConfiguration.setTypeConfigurations(Arrays.asList(binaryTypeConfigurationForStoreRef));
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

            final CacheConfiguration<Long, StoreEntity> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("stores");
            cacheConfig.setDataRegionName("comparison");
            final IgniteCache<Long, StoreEntity> referenceCache = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<Long, StoreEntity> cache = grid.getOrCreateCache(cacheConfig);

            this.efficiencyImpl(referenceGrid, grid, referenceCache, cache, "aldica optimised", "Ignite default", 0.13);
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

            final CacheConfiguration<Long, StoreEntity> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("stores");
            cacheConfig.setDataRegionName("comparison");
            final IgniteCache<Long, StoreEntity> referenceCache = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<Long, StoreEntity> cache = grid.getOrCreateCache(cacheConfig);

            // we optimise serial form for most value components, so quite a bit of difference - 12%
            this.efficiencyImpl(referenceGrid, grid, referenceCache, cache, "aldica raw serial", "aldica optimised", 0.12);
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
            final CacheConfiguration<Long, StoreEntity> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setName("stores");
            cacheConfig.setCacheMode(CacheMode.LOCAL);
            final IgniteCache<Long, StoreEntity> cache = grid.getOrCreateCache(cacheConfig);

            StoreEntity controlValue;
            StoreEntity cacheValue;

            controlValue = new StoreEntity();
            controlValue.setId(1l);
            controlValue.setVersion(1l);
            controlValue.setProtocol(StoreRef.PROTOCOL_WORKSPACE);
            controlValue.setIdentifier("SpacesStore");
            controlValue.setRootNode(new NodeEntity());

            cache.put(1l, controlValue);
            cacheValue = cache.get(1l);

            // can't check for equals - value class does not support it
            // check deep serialisation was actually involved (different value instances)
            Assert.assertNotSame(controlValue, cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getVersion(), cacheValue.getVersion());
            Assert.assertEquals(controlValue.getProtocol(), cacheValue.getProtocol());
            Assert.assertEquals(controlValue.getIdentifier(), cacheValue.getIdentifier());
            Assert.assertNotNull(cacheValue.getRootNode());

            controlValue = new StoreEntity();
            controlValue.setId(2l);
            controlValue.setVersion(1l);
            controlValue.setProtocol(StoreRef.PROTOCOL_WORKSPACE);
            controlValue.setIdentifier("custom");

            cache.put(2l, controlValue);
            cacheValue = cache.get(2l);

            // can't check for equals - value class does not support it
            // check deep serialisation was actually involved (different value instances)
            Assert.assertNotSame(controlValue, cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getVersion(), cacheValue.getVersion());
            Assert.assertEquals(controlValue.getProtocol(), cacheValue.getProtocol());
            Assert.assertEquals(controlValue.getIdentifier(), cacheValue.getIdentifier());
            Assert.assertSame(controlValue.getRootNode(), cacheValue.getRootNode());

            controlValue = new StoreEntity();
            controlValue.setId(3l);
            controlValue.setVersion(1l);
            controlValue.setProtocol("custom");
            controlValue.setIdentifier("SpacesStore");

            cache.put(3l, controlValue);
            cacheValue = cache.get(3l);

            // can't check for equals - value class does not support it
            // check deep serialisation was actually involved (different value instances)
            Assert.assertNotSame(controlValue, cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getVersion(), cacheValue.getVersion());
            Assert.assertEquals(controlValue.getProtocol(), cacheValue.getProtocol());
            Assert.assertEquals(controlValue.getIdentifier(), cacheValue.getIdentifier());
            Assert.assertSame(controlValue.getRootNode(), cacheValue.getRootNode());

            controlValue = new StoreEntity();
            controlValue.setId(4l);
            controlValue.setVersion(1l);
            controlValue.setProtocol("custom");
            controlValue.setIdentifier("custom");

            cache.put(4l, controlValue);
            cacheValue = cache.get(4l);

            // can't check for equals - value class does not support it
            // check deep serialisation was actually involved (different value instances)
            Assert.assertNotSame(controlValue, cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getVersion(), cacheValue.getVersion());
            Assert.assertEquals(controlValue.getProtocol(), cacheValue.getProtocol());
            Assert.assertEquals(controlValue.getIdentifier(), cacheValue.getIdentifier());
            Assert.assertSame(controlValue.getRootNode(), cacheValue.getRootNode());
        }
    }

    protected void efficiencyImpl(final Ignite referenceGrid, final Ignite grid, final IgniteCache<Long, StoreEntity> referenceCache,
            final IgniteCache<Long, StoreEntity> cache, final String serialisationType, final String referenceSerialisationType,
            final double marginFraction)
    {
        LOGGER.info(
                "Running StoreEntity serialisation benchmark of 100k instances, comparing {} vs. {} serialisation, expecting relative improvement margin / difference fraction of {}",
                referenceSerialisationType, serialisationType, marginFraction);

        final SecureRandom rnJesus = new SecureRandom();

        for (int idx = 0; idx < 100000; idx++)
        {
            final StoreEntity value = new StoreEntity();
            value.setId(Long.valueOf(idx));
            value.setVersion(Long.valueOf(rnJesus.nextInt(1024)));
            value.setProtocol(PROTOCOLS[rnJesus.nextInt(PROTOCOLS.length)]);
            value.setIdentifier(IDENTIFIERS[rnJesus.nextInt(IDENTIFIERS.length)]);

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
