/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
package org.aldica.repo.ignite.binary.support;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.aldica.common.ignite.GridTestsBase;
import org.aldica.repo.ignite.ExpensiveTestCategory;
import org.alfresco.error.AlfrescoRuntimeException;
import org.alfresco.model.ContentModel;
import org.alfresco.model.RenditionModel;
import org.alfresco.repo.domain.qname.QNameDAO;
import org.alfresco.repo.domain.qname.ibatis.QNameDAOImpl;
import org.alfresco.service.namespace.QName;
import org.alfresco.util.Pair;
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
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;

/**
 * @author Axel Faust
 */
public class ChildByNameKeyBinarySerializerTests extends GridTestsBase
{

    private static final Class<?> CHILD_BY_NAME_KEY_CLASS;

    private static final Constructor<?> CHILD_BY_NAME_KEY_CTOR;

    private static final Field PARENT_NODE_ID_FIELD;

    private static final Field ASSOC_TYPE_QNAME_FIELD;

    private static final Field CHILD_NODE_NAME_FIELD;

    static
    {
        try
        {
            // class is package protected
            CHILD_BY_NAME_KEY_CLASS = Class.forName("org.alfresco.repo.domain.node.ChildByNameKey");
            CHILD_BY_NAME_KEY_CTOR = CHILD_BY_NAME_KEY_CLASS.getDeclaredConstructor(Long.class, QName.class, String.class);
            PARENT_NODE_ID_FIELD = CHILD_BY_NAME_KEY_CLASS.getDeclaredField("parentNodeId");
            ASSOC_TYPE_QNAME_FIELD = CHILD_BY_NAME_KEY_CLASS.getDeclaredField("assocTypeQName");
            CHILD_NODE_NAME_FIELD = CHILD_BY_NAME_KEY_CLASS.getDeclaredField("childNodeName");

            CHILD_BY_NAME_KEY_CTOR.setAccessible(true);
            PARENT_NODE_ID_FIELD.setAccessible(true);
            ASSOC_TYPE_QNAME_FIELD.setAccessible(true);
            CHILD_NODE_NAME_FIELD.setAccessible(true);
        }
        catch (final ClassNotFoundException | NoSuchMethodException | NoSuchFieldException e)
        {
            throw new AlfrescoRuntimeException("Failed to lookup class / constructor / fields");
        }
    }

    private static final QName[] ASSOC_TYPE_QNAMES = { ContentModel.ASSOC_CONTAINS, ContentModel.ASSOC_CHILDREN,
            ContentModel.ASSOC_ATTACHMENTS, ContentModel.ASSOC_AVATAR, ContentModel.ASSOC_IN_ZONE, ContentModel.ASSOC_RATINGS };

    private static final Logger LOGGER = LoggerFactory.getLogger(ChildByNameKeyBinarySerializerTests.class);

    protected static GenericApplicationContext createApplicationContext()
    {
        final GenericApplicationContext appContext = new GenericApplicationContext();

        final QNameDAO qnameDAO = EasyMock.partialMockBuilder(QNameDAOImpl.class).addMockedMethod("getQName", Long.class)
                .addMockedMethod("getQName", QName.class).createMock();
        appContext.getBeanFactory().registerSingleton("qnameDAO", qnameDAO);
        appContext.refresh();

        for (int idx = 0; idx < ASSOC_TYPE_QNAMES.length; idx++)
        {
            EasyMock.expect(qnameDAO.getQName(Long.valueOf(idx))).andStubReturn(new Pair<>(Long.valueOf(idx), ASSOC_TYPE_QNAMES[idx]));
            EasyMock.expect(qnameDAO.getQName(ASSOC_TYPE_QNAMES[idx])).andStubReturn(new Pair<>(Long.valueOf(idx), ASSOC_TYPE_QNAMES[idx]));
        }
        EasyMock.expect(qnameDAO.getQName(EasyMock.anyObject(QName.class))).andStubReturn(null);

        EasyMock.replay(qnameDAO);

        return appContext;
    }

    protected static IgniteConfiguration createConfiguration(final ApplicationContext applicationContext, final boolean idsWhenReasonable,
            final boolean serialForm, final String... regionNames)
    {
        final IgniteConfiguration conf = createConfiguration(1, false, null);

        final BinaryConfiguration binaryConfiguration = new BinaryConfiguration();

        final BinaryTypeConfiguration binaryTypeConfigurationForChildByNameKey = new BinaryTypeConfiguration();
        binaryTypeConfigurationForChildByNameKey.setTypeName(CHILD_BY_NAME_KEY_CLASS.getName());
        final ChildByNameKeyBinarySerializer serializer = new ChildByNameKeyBinarySerializer();
        serializer.setApplicationContext(applicationContext);
        serializer.setUseRawSerialForm(serialForm);
        serializer.setUseVariableLengthIntegers(serialForm);
        serializer.setUseIdsWhenReasonable(idsWhenReasonable);
        binaryTypeConfigurationForChildByNameKey.setSerializer(serializer);

        binaryConfiguration.setTypeConfigurations(Arrays.asList(binaryTypeConfigurationForChildByNameKey));
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
        final IgniteConfiguration conf = createConfiguration(null, false, false);
        this.correctnessImpl(conf);
    }

    @Test
    public void defaultFormQNameIdSubstitutionCorrectness() throws Exception
    {
        try (final GenericApplicationContext appContext = createApplicationContext())
        {
            final IgniteConfiguration conf = createConfiguration(appContext, true, false);
            this.correctnessImpl(conf);
        }
    }

    @Category(ExpensiveTestCategory.class)
    @Test
    public void defaultFormEfficiency() throws Exception
    {
        try (final GenericApplicationContext appContext = createApplicationContext())
        {
            final IgniteConfiguration referenceConf = createConfiguration(1, false, null);
            referenceConf.setIgniteInstanceName(referenceConf.getIgniteInstanceName() + "-reference");

            final IgniteConfiguration defaultConf = createConfiguration(null, false, false, "comparison1", "comparison2", "comparison3");
            final IgniteConfiguration useQNameIdConf = createConfiguration(appContext, true, false, "comparison1", "comparison2",
                    "comparison3");

            useQNameIdConf.setIgniteInstanceName(useQNameIdConf.getIgniteInstanceName() + "-qnameIdSubstitution");

            referenceConf.setDataStorageConfiguration(defaultConf.getDataStorageConfiguration());

            try
            {
                final Ignite referenceGrid = Ignition.start(referenceConf);
                final Ignite defaultGrid = Ignition.start(defaultConf);
                final Ignite useQNameIdGrid = Ignition.start(useQNameIdConf);

                final CacheConfiguration<Long, Object> cacheConfig = new CacheConfiguration<>();
                cacheConfig.setCacheMode(CacheMode.LOCAL);

                cacheConfig.setName("comparison1");
                cacheConfig.setDataRegionName("comparison1");
                final IgniteCache<Long, Object> referenceCache1 = referenceGrid.getOrCreateCache(cacheConfig);
                final IgniteCache<Long, Object> cache1 = defaultGrid.getOrCreateCache(cacheConfig);

                // minor drawback due to extra flag - -5%
                this.efficiencyImpl(referenceGrid, defaultGrid, referenceCache1, cache1, "aldica optimised", "Ignite default", -0.05);

                cacheConfig.setName("comparison2");
                cacheConfig.setDataRegionName("comparison2");
                final IgniteCache<Long, Object> referenceCache2 = referenceGrid.getOrCreateCache(cacheConfig);
                final IgniteCache<Long, Object> cache2 = useQNameIdGrid.getOrCreateCache(cacheConfig);

                // significant difference (QName is a large part of key) - 32%
                this.efficiencyImpl(referenceGrid, useQNameIdGrid, referenceCache2, cache2, "aldica optimised (QName ID substitution)",
                        "Ignite default", 0.32);

                cacheConfig.setName("comparison3");
                cacheConfig.setDataRegionName("comparison3");
                final IgniteCache<Long, Object> referenceCache3 = defaultGrid.getOrCreateCache(cacheConfig);
                final IgniteCache<Long, Object> cache3 = useQNameIdGrid.getOrCreateCache(cacheConfig);

                // significant difference (QName is a large part of key) - 21%
                this.efficiencyImpl(defaultGrid, useQNameIdGrid, referenceCache3, cache3, "aldica optimised (QName ID substitution)",
                        "aldica optimised", 0.21);
            }
            finally
            {
                Ignition.stopAll(true);
            }
        }
    }

    @Test
    public void rawSerialFormCorrectness() throws Exception
    {
        final IgniteConfiguration conf = createConfiguration(null, false, true);
        this.correctnessImpl(conf);
    }

    @Test
    public void rawSerialFormQNameIdSubstitutionCorrectness() throws Exception
    {
        try (final GenericApplicationContext appContext = createApplicationContext())
        {
            final IgniteConfiguration conf = createConfiguration(appContext, true, true);
            this.correctnessImpl(conf);
        }
    }

    @Category(ExpensiveTestCategory.class)
    @Test
    public void rawSerialFormEfficiency() throws Exception
    {
        try (final GenericApplicationContext appContext = createApplicationContext())
        {
            final IgniteConfiguration referenceConf = createConfiguration(1, false, null);
            referenceConf.setIgniteInstanceName(referenceConf.getIgniteInstanceName() + "-reference");

            final IgniteConfiguration defaultConf = createConfiguration(null, false, true, "comparison1", "comparison2", "comparison3");
            final IgniteConfiguration useQNameIdConf = createConfiguration(appContext, true, true, "comparison1", "comparison2",
                    "comparison3");

            useQNameIdConf.setIgniteInstanceName(useQNameIdConf.getIgniteInstanceName() + "-qnameIdSubstitution");

            referenceConf.setDataStorageConfiguration(defaultConf.getDataStorageConfiguration());

            try
            {
                final Ignite referenceGrid = Ignition.start(referenceConf);
                final Ignite defaultGrid = Ignition.start(defaultConf);
                final Ignite useQNameIdGrid = Ignition.start(useQNameIdConf);

                final CacheConfiguration<Long, Object> cacheConfig = new CacheConfiguration<>();
                cacheConfig.setCacheMode(CacheMode.LOCAL);

                cacheConfig.setName("comparison1");
                cacheConfig.setDataRegionName("comparison1");
                final IgniteCache<Long, Object> referenceCache1 = referenceGrid.getOrCreateCache(cacheConfig);
                final IgniteCache<Long, Object> cache1 = defaultGrid.getOrCreateCache(cacheConfig);

                // slight improvement due to variable length integers - 4%
                this.efficiencyImpl(referenceGrid, defaultGrid, referenceCache1, cache1, "aldica raw serial", "aldica optimised", 0.04);

                cacheConfig.setName("comparison2");
                cacheConfig.setDataRegionName("comparison2");
                final IgniteCache<Long, Object> referenceCache2 = referenceGrid.getOrCreateCache(cacheConfig);
                final IgniteCache<Long, Object> cache2 = useQNameIdGrid.getOrCreateCache(cacheConfig);

                // significant difference (QName is a large part of key) - 40%
                this.efficiencyImpl(referenceGrid, useQNameIdGrid, referenceCache2, cache2, "aldica raw serial (QName ID substitution)",
                        "aldica optimised", 0.40);

                cacheConfig.setName("comparison3");
                cacheConfig.setDataRegionName("comparison3");
                final IgniteCache<Long, Object> referenceCache3 = defaultGrid.getOrCreateCache(cacheConfig);
                final IgniteCache<Long, Object> cache3 = useQNameIdGrid.getOrCreateCache(cacheConfig);

                // significant difference (QName is a large part of key) - 38%
                this.efficiencyImpl(defaultGrid, useQNameIdGrid, referenceCache3, cache3, "aldica raw serial (QName ID substitution)",
                        "aldica raw serial", 0.38);
            }
            finally
            {
                Ignition.stopAll(true);
            }
        }
    }

    protected void correctnessImpl(final IgniteConfiguration conf)
            throws InstantiationException, InvocationTargetException, IllegalAccessException
    {
        try (Ignite grid = Ignition.start(conf))
        {
            final CacheConfiguration<Long, Object> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setName("childByNameKey");
            cacheConfig.setCacheMode(CacheMode.LOCAL);
            final IgniteCache<Long, Object> cache = grid.getOrCreateCache(cacheConfig);

            Object controlValue;
            Object cacheValue;

            controlValue = CHILD_BY_NAME_KEY_CTOR.newInstance(Long.valueOf(1l), ContentModel.ASSOC_CONTAINS, "Company Home");
            cache.put(1l, controlValue);

            cacheValue = cache.get(1l);

            Assert.assertEquals(controlValue, cacheValue);
            // check deep serialisation was actually involved
            Assert.assertNotSame(controlValue, cacheValue);

            // association not covered by QNameDAO
            controlValue = CHILD_BY_NAME_KEY_CTOR.newInstance(Long.valueOf(1l), RenditionModel.ASSOC_RENDITION, "System");
            cache.put(2l, controlValue);

            cacheValue = cache.get(2l);

            Assert.assertEquals(controlValue, cacheValue);
            // check deep serialisation was actually involved
            Assert.assertNotSame(controlValue, cacheValue);
        }
    }

    protected void efficiencyImpl(final Ignite referenceGrid, final Ignite grid, final IgniteCache<Long, Object> referenceCache,
            final IgniteCache<Long, Object> cache, final String serialisationType, final String referenceSerialisationType,
            final double marginFraction) throws InstantiationException, InvocationTargetException, IllegalAccessException
    {
        LOGGER.info(
                "Running ChildByNameKey serialisation benchmark of 100k instances, comparing {} vs. {} serialisation, expecting relative improvement margin / difference fraction of {}",
                referenceSerialisationType, serialisationType, marginFraction);

        final SecureRandom rnJesus = new SecureRandom();
        for (int idx = 0; idx < 100000; idx++)
        {
            final long id = idx;
            final QName assocTypeQName = ASSOC_TYPE_QNAMES[rnJesus.nextInt(ASSOC_TYPE_QNAMES.length)];
            final String childNodeName = UUID.randomUUID().toString();

            final Object value = CHILD_BY_NAME_KEY_CTOR.newInstance(id, assocTypeQName, childNodeName);

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
