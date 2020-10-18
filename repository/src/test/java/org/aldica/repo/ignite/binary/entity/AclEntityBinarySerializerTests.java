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
import org.alfresco.repo.domain.permissions.AclEntity;
import org.alfresco.repo.security.permissions.ACLType;
import org.alfresco.util.GUID;
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
public class AclEntityBinarySerializerTests extends GridTestsBase
{

    private static final Logger LOGGER = LoggerFactory.getLogger(AclEntityBinarySerializerTests.class);

    protected static IgniteConfiguration createConfiguration(final boolean serialForm, final String... regionNames)
    {
        final IgniteConfiguration conf = createConfiguration(1, false, null);

        final BinaryConfiguration binaryConfiguration = new BinaryConfiguration();

        final BinaryTypeConfiguration binaryTypeConfigurationForAclEntity = new BinaryTypeConfiguration();
        binaryTypeConfigurationForAclEntity.setTypeName(AclEntity.class.getName());
        final AclEntityBinarySerializer serializer = new AclEntityBinarySerializer();
        serializer.setUseRawSerialForm(serialForm);
        serializer.setUseVariableLengthIntegers(serialForm);
        binaryTypeConfigurationForAclEntity.setSerializer(serializer);

        binaryConfiguration.setTypeConfigurations(Arrays.asList(binaryTypeConfigurationForAclEntity));
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

            final CacheConfiguration<Long, AclEntity> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("acls");
            cacheConfig.setDataRegionName("comparison");
            final IgniteCache<Long, AclEntity> referenceCache = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<Long, AclEntity> cache = grid.getOrCreateCache(cacheConfig);

            // ACL ID serialisation and bool-type aggregation should already save a bit - 15%
            this.efficiencyImpl(referenceGrid, grid, referenceCache, cache, "aldica optimised", "Ignite default", 0.15);
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

            final CacheConfiguration<Long, AclEntity> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("acls");
            cacheConfig.setDataRegionName("comparison");
            final IgniteCache<Long, AclEntity> referenceCache = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<Long, AclEntity> cache = grid.getOrCreateCache(cacheConfig);

            // we optimise serial form for most value components, so still quite a bit of difference compared to the already optimised
            // aldica default - 18%
            this.efficiencyImpl(referenceGrid, grid, referenceCache, cache, "aldica raw serial", "aldica optimised", 0.18);
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
            final CacheConfiguration<Long, AclEntity> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setName("acls");
            cacheConfig.setCacheMode(CacheMode.LOCAL);
            final IgniteCache<Long, AclEntity> cache = grid.getOrCreateCache(cacheConfig);

            AclEntity controlValue;
            AclEntity cacheValue;

            controlValue = new AclEntity();
            controlValue.setId(1l);
            controlValue.setVersion(1l);
            controlValue.setAclId(GUID.generate());
            controlValue.setLatest(true);
            controlValue.setAclVersion(1l);
            controlValue.setInherits(false);
            controlValue.setInheritsFrom(null);
            controlValue.setAclType(ACLType.DEFINING);
            controlValue.setInheritedAcl(2l);
            controlValue.setVersioned(false);
            controlValue.setRequiresVersion(false);
            controlValue.setAclChangeSetId(1l);

            cache.put(1l, controlValue);
            cacheValue = cache.get(1l);

            // don't check for equals - value class only has superficial check
            // check deep serialisation was actually involved (different value instances)
            Assert.assertNotSame(controlValue, cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getVersion(), cacheValue.getVersion());
            Assert.assertEquals(controlValue.getAclId(), cacheValue.getAclId());
            Assert.assertEquals(controlValue.getInherits(), cacheValue.getInherits());
            Assert.assertNull(cacheValue.getInheritsFrom());
            Assert.assertEquals(controlValue.getType(), cacheValue.getType());
            Assert.assertEquals(controlValue.getInheritedAcl(), cacheValue.getInheritedAcl());
            Assert.assertEquals(controlValue.isVersioned(), cacheValue.isVersioned());
            Assert.assertEquals(controlValue.getRequiresVersion(), cacheValue.getRequiresVersion());
            Assert.assertEquals(controlValue.getAclChangeSetId(), cacheValue.getAclChangeSetId());

            controlValue = new AclEntity();
            controlValue.setId(2l);
            controlValue.setVersion(1l);
            controlValue.setAclId(GUID.generate());
            controlValue.setLatest(true);
            controlValue.setAclVersion(1l);
            controlValue.setInherits(true);
            controlValue.setInheritsFrom(1l);
            controlValue.setAclType(ACLType.SHARED);
            controlValue.setInheritedAcl(2l);
            controlValue.setVersioned(false);
            controlValue.setRequiresVersion(false);
            controlValue.setAclChangeSetId(1l);

            cache.put(2l, controlValue);
            cacheValue = cache.get(2l);

            // don't check for equals - value class only has superficial check
            // check deep serialisation was actually involved (different value instances)
            Assert.assertNotSame(controlValue, cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getVersion(), cacheValue.getVersion());
            Assert.assertEquals(controlValue.getAclId(), cacheValue.getAclId());
            Assert.assertEquals(controlValue.getInherits(), cacheValue.getInherits());
            Assert.assertEquals(controlValue.getInheritsFrom(), cacheValue.getInheritsFrom());
            Assert.assertEquals(controlValue.getType(), cacheValue.getType());
            Assert.assertEquals(controlValue.getInheritedAcl(), cacheValue.getInheritedAcl());
            Assert.assertEquals(controlValue.isVersioned(), cacheValue.isVersioned());
            Assert.assertEquals(controlValue.getRequiresVersion(), cacheValue.getRequiresVersion());
            Assert.assertEquals(controlValue.getAclChangeSetId(), cacheValue.getAclChangeSetId());

            // all nullable fields nulled and booleans inverted from previous
            controlValue = new AclEntity();
            controlValue.setId(3l);
            controlValue.setVersion(1l);
            controlValue.setAclId(GUID.generate());
            controlValue.setLatest(true);
            controlValue.setAclVersion(1l);
            controlValue.setInherits(false);
            controlValue.setInheritsFrom(null);
            controlValue.setAclType(ACLType.FIXED);
            controlValue.setInheritedAcl(null);
            controlValue.setVersioned(true);
            controlValue.setRequiresVersion(true);
            controlValue.setAclChangeSetId(null);

            cache.put(3l, controlValue);
            cacheValue = cache.get(3l);

            // don't check for equals - value class only has superficial check
            // check deep serialisation was actually involved (different value instances)
            Assert.assertNotSame(controlValue, cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getVersion(), cacheValue.getVersion());
            Assert.assertEquals(controlValue.getAclId(), cacheValue.getAclId());
            Assert.assertEquals(controlValue.getInherits(), cacheValue.getInherits());
            Assert.assertNull(cacheValue.getInheritsFrom());
            Assert.assertEquals(controlValue.getType(), cacheValue.getType());
            Assert.assertNull(cacheValue.getInheritedAcl());
            Assert.assertEquals(controlValue.isVersioned(), cacheValue.isVersioned());
            Assert.assertEquals(controlValue.getRequiresVersion(), cacheValue.getRequiresVersion());
            Assert.assertNull(cacheValue.getAclChangeSetId());

            // all nullable fields set
            controlValue = new AclEntity();
            controlValue.setId(4l);
            controlValue.setVersion(1l);
            controlValue.setAclId(GUID.generate());
            controlValue.setLatest(true);
            controlValue.setAclVersion(1l);
            controlValue.setInherits(false);
            controlValue.setInheritsFrom(999l);
            controlValue.setAclType(ACLType.GLOBAL);
            controlValue.setInheritedAcl(999l);
            controlValue.setVersioned(true);
            controlValue.setRequiresVersion(true);
            controlValue.setAclChangeSetId(999l);

            cache.put(4l, controlValue);
            cacheValue = cache.get(4l);

            // don't check for equals - value class only has superficial check
            // check deep serialisation was actually involved (different value instances)
            Assert.assertNotSame(controlValue, cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getVersion(), cacheValue.getVersion());
            Assert.assertEquals(controlValue.getAclId(), cacheValue.getAclId());
            Assert.assertEquals(controlValue.getInherits(), cacheValue.getInherits());
            Assert.assertEquals(controlValue.getInheritsFrom(), cacheValue.getInheritsFrom());
            Assert.assertEquals(controlValue.getType(), cacheValue.getType());
            Assert.assertEquals(controlValue.getInheritedAcl(), cacheValue.getInheritedAcl());
            Assert.assertEquals(controlValue.isVersioned(), cacheValue.isVersioned());
            Assert.assertEquals(controlValue.getRequiresVersion(), cacheValue.getRequiresVersion());
            Assert.assertEquals(controlValue.getAclChangeSetId(), cacheValue.getAclChangeSetId());
        }
    }

    protected void efficiencyImpl(final Ignite referenceGrid, final Ignite grid, final IgniteCache<Long, AclEntity> referenceCache,
            final IgniteCache<Long, AclEntity> cache, final String serialisationType, final String referenceSerialisationType,
            final double marginFraction)
    {
        LOGGER.info(
                "Running AclEntity serialisation benchmark of 100k instances, comparing {} vs. {} serialisation, expecting relative improvement margin / difference fraction of {}",
                referenceSerialisationType, serialisationType, marginFraction);

        final SecureRandom rnJesus = new SecureRandom();

        for (int idx = 10; idx < 100010; idx++)
        {
            final AclEntity value = new AclEntity();
            value.setId(Long.valueOf(idx));
            value.setVersion(Long.valueOf(rnJesus.nextInt(1024)));
            value.setAclId(GUID.generate());
            value.setLatest(true);
            value.setAclVersion(Long.valueOf(rnJesus.nextInt(128)));
            value.setInherits(true);
            value.setInheritsFrom(Long.valueOf(rnJesus.nextInt(idx)));
            value.setAclType(ACLType.SHARED);
            value.setInheritedAcl(Long.valueOf(idx));
            value.setVersioned(false);
            value.setRequiresVersion(false);
            value.setAclChangeSetId(Long.valueOf(idx));

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
