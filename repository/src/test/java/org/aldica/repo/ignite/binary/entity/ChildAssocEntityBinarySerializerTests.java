/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
package org.aldica.repo.ignite.binary.entity;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.aldica.common.ignite.GridTestsBase;
import org.aldica.repo.ignite.ExpensiveTestCategory;
import org.alfresco.repo.domain.node.ChildAssocEntity;
import org.alfresco.repo.domain.node.NodeEntity;
import org.alfresco.service.namespace.NamespaceService;
import org.alfresco.service.namespace.QName;
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
public class ChildAssocEntityBinarySerializerTests extends GridTestsBase
{

    private static final Logger LOGGER = LoggerFactory.getLogger(ChildAssocEntityBinarySerializerTests.class);

    protected static IgniteConfiguration createConfiguration(final boolean serialForm, final String... regionNames)
    {
        final IgniteConfiguration conf = createConfiguration(1, false, null);

        final BinaryConfiguration binaryConfiguration = new BinaryConfiguration();

        final BinaryTypeConfiguration binaryTypeConfigurationForChildAssocEntity = new BinaryTypeConfiguration();
        binaryTypeConfigurationForChildAssocEntity.setTypeName(ChildAssocEntity.class.getName());
        final ChildAssocEntityBinarySerializer serializer = new ChildAssocEntityBinarySerializer();
        serializer.setUseRawSerialForm(serialForm);
        serializer.setUseVariableLengthIntegers(serialForm);
        binaryTypeConfigurationForChildAssocEntity.setSerializer(serializer);

        binaryConfiguration.setTypeConfigurations(Arrays.asList(binaryTypeConfigurationForChildAssocEntity));
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

            final CacheConfiguration<Long, ChildAssocEntity> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("childAssocs");
            cacheConfig.setDataRegionName("comparison");
            final IgniteCache<Long, ChildAssocEntity> referenceCache = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<Long, ChildAssocEntity> cache = grid.getOrCreateCache(cacheConfig);

            // slight savings due to reduced field metadata - 8%
            this.efficiencyImpl(referenceGrid, grid, referenceCache, cache, "aldica optimised", "Ignite default", 0.08);
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

            final CacheConfiguration<Long, ChildAssocEntity> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("childAssocs");
            cacheConfig.setDataRegionName("comparison");
            final IgniteCache<Long, ChildAssocEntity> referenceCache = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<Long, ChildAssocEntity> cache = grid.getOrCreateCache(cacheConfig);

            // variable length integers / optimised strings - further 15 %
            this.efficiencyImpl(referenceGrid, grid, referenceCache, cache, "aldica raw serial", "aldica optimised", 0.15);
        }
        finally
        {
            Ignition.stopAll(true);
        }
    }

    @SuppressWarnings("deprecation")
    protected void correctnessImpl(final IgniteConfiguration conf)
    {
        try (Ignite grid = Ignition.start(conf))
        {
            final CacheConfiguration<Long, ChildAssocEntity> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setName("nodes");
            cacheConfig.setCacheMode(CacheMode.LOCAL);
            final IgniteCache<Long, ChildAssocEntity> cache = grid.getOrCreateCache(cacheConfig);

            ChildAssocEntity controlValue;
            ChildAssocEntity cacheValue;
            final NodeEntity parentNodeControlValue;
            NodeEntity parentNodeCacheValue;
            final NodeEntity childNodeControlValue;
            NodeEntity childNodeCacheValue;

            // we don't really need different entities for parent / child node
            // their serialisation is tested in detail in other tests
            parentNodeControlValue = new NodeEntity();
            parentNodeControlValue.setId(1l);

            childNodeControlValue = new NodeEntity();
            childNodeControlValue.setId(2l);

            controlValue = new ChildAssocEntity();
            controlValue.setId(1l);
            controlValue.setVersion(1l);
            controlValue.setParentNode(parentNodeControlValue);
            controlValue.setChildNode(childNodeControlValue);
            controlValue.setTypeQNameId(1l);
            controlValue.setChildNodeName("childFolder");
            controlValue.setChildNodeNameCrc(ChildAssocEntity.getChildNodeNameCrc(controlValue.getChildNodeName()));
            controlValue.setQnameNamespaceId(1l);
            controlValue.setQnameLocalName("childFolder");
            controlValue.setQnameCrc(ChildAssocEntity
                    .getQNameCrc(QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, controlValue.getQnameLocalName())));
            controlValue.setPrimary(Boolean.TRUE);
            controlValue.setAssocIndex(-1);

            cache.put(1l, controlValue);
            cacheValue = cache.get(1l);
            parentNodeCacheValue = cacheValue.getParentNode();
            childNodeCacheValue = cacheValue.getChildNode();

            // can't check for equals - value class does not offer equals
            // check deep serialisation was actually involved (different value instances)
            Assert.assertNotSame(controlValue, cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getVersion(), cacheValue.getVersion());
            Assert.assertNotSame(parentNodeControlValue, parentNodeCacheValue);
            Assert.assertEquals(parentNodeControlValue.getId(), parentNodeCacheValue.getId());
            Assert.assertNotSame(childNodeControlValue, childNodeCacheValue);
            Assert.assertEquals(childNodeControlValue.getId(), childNodeCacheValue.getId());
            Assert.assertEquals(controlValue.getTypeQNameId(), cacheValue.getTypeQNameId());
            Assert.assertEquals(controlValue.getChildNodeName(), cacheValue.getChildNodeName());
            Assert.assertEquals(controlValue.getChildNodeNameCrc(), cacheValue.getChildNodeNameCrc());
            Assert.assertEquals(controlValue.getQnameNamespaceId(), cacheValue.getQnameNamespaceId());
            Assert.assertEquals(controlValue.getQnameLocalName(), cacheValue.getQnameLocalName());
            Assert.assertEquals(controlValue.getQnameCrc(), cacheValue.getQnameCrc());
            Assert.assertEquals(controlValue.isPrimary(), cacheValue.isPrimary());
            Assert.assertEquals(controlValue.getAssocIndex(), cacheValue.getAssocIndex());

            controlValue = new ChildAssocEntity();
            controlValue.setId(2l);
            controlValue.setVersion(null);
            controlValue.setParentNode(parentNodeControlValue);
            controlValue.setChildNode(childNodeControlValue);
            controlValue.setTypeQNameId(987654321l);
            controlValue.setChildNodeName(UUID.randomUUID().toString());
            controlValue.setChildNodeNameCrc(ChildAssocEntity.getChildNodeNameCrc(controlValue.getChildNodeName()));
            controlValue.setQnameNamespaceId(1l);
            controlValue.setQnameLocalName(UUID.randomUUID().toString());
            controlValue.setQnameCrc(ChildAssocEntity
                    .getQNameCrc(QName.createQName(NamespaceService.SYSTEM_MODEL_1_0_URI, controlValue.getQnameLocalName())));
            controlValue.setPrimary(null);
            controlValue.setAssocIndex(123456789);

            cache.put(2l, controlValue);
            cacheValue = cache.get(2l);
            parentNodeCacheValue = cacheValue.getParentNode();
            childNodeCacheValue = cacheValue.getChildNode();

            // can't check for equals - value class does not offer equals
            // check deep serialisation was actually involved (different value instances)
            Assert.assertNotSame(controlValue, cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getVersion(), cacheValue.getVersion());
            Assert.assertNotSame(parentNodeControlValue, parentNodeCacheValue);
            Assert.assertEquals(parentNodeControlValue.getId(), parentNodeCacheValue.getId());
            Assert.assertNotSame(childNodeControlValue, childNodeCacheValue);
            Assert.assertEquals(childNodeControlValue.getId(), childNodeCacheValue.getId());
            Assert.assertEquals(controlValue.getTypeQNameId(), cacheValue.getTypeQNameId());
            Assert.assertEquals(controlValue.getChildNodeName(), cacheValue.getChildNodeName());
            Assert.assertEquals(controlValue.getChildNodeNameCrc(), cacheValue.getChildNodeNameCrc());
            Assert.assertEquals(controlValue.getQnameNamespaceId(), cacheValue.getQnameNamespaceId());
            Assert.assertEquals(controlValue.getQnameLocalName(), cacheValue.getQnameLocalName());
            Assert.assertEquals(controlValue.getQnameCrc(), cacheValue.getQnameCrc());
            Assert.assertEquals(controlValue.isPrimary(), cacheValue.isPrimary());
            Assert.assertEquals(controlValue.getAssocIndex(), cacheValue.getAssocIndex());
        }
    }

    @SuppressWarnings("deprecation")
    protected void efficiencyImpl(final Ignite referenceGrid, final Ignite grid, final IgniteCache<Long, ChildAssocEntity> referenceCache,
            final IgniteCache<Long, ChildAssocEntity> cache, final String serialisationType, final String referenceSerialisationType,
            final double marginFraction)
    {
        LOGGER.info(
                "Running ChildAssocEntity serialisation benchmark of 100k instances, comparing {} vs. {} serialisation, expecting relative improvement margin / difference fraction of {}",
                referenceSerialisationType, serialisationType, marginFraction);

        final NodeEntity parentNode;
        final NodeEntity childNode;

        // we don't really need different entities for parent / child node
        // their serialisation is benchmarked in other tests
        parentNode = new NodeEntity();
        parentNode.setId(1l);

        childNode = new NodeEntity();
        childNode.setId(2l);

        final SecureRandom rnJesus = new SecureRandom();

        for (int idx = 0; idx < 100000; idx++)
        {
            final ChildAssocEntity value = new ChildAssocEntity();

            value.setId((long) idx);
            // versions on child assocs typically have very low values
            value.setVersion((long) rnJesus.nextInt(10));
            value.setParentNode(parentNode);
            value.setChildNode(childNode);
            // most systems should have no more than 3000 distinct class / feature qnames
            // ~300 is the default in a fresh system
            value.setTypeQNameId((long) rnJesus.nextInt(3000));
            value.setChildNodeName(UUID.randomUUID().toString());
            value.setChildNodeNameCrc(ChildAssocEntity.getChildNodeNameCrc(value.getChildNodeName()));
            // most systems should have no more than 50 model namespaces
            // ~25 is the default in a fresh system
            value.setQnameNamespaceId((long) rnJesus.nextInt(50));
            value.setQnameLocalName(UUID.randomUUID().toString());
            value.setQnameCrc(
                    ChildAssocEntity.getQNameCrc(QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, value.getQnameLocalName())));
            value.setPrimary(Boolean.TRUE);
            // -1 is the most common assoc index (in some systems the only one)
            value.setAssocIndex(-1);

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
