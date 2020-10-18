/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
package org.aldica.repo.ignite.binary.entity;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.aldica.common.ignite.GridTestsBase;
import org.aldica.repo.ignite.ExpensiveTestCategory;
import org.alfresco.repo.domain.node.AuditablePropertiesEntity;
import org.alfresco.repo.domain.node.NodeEntity;
import org.alfresco.repo.domain.node.NodeUpdateEntity;
import org.alfresco.repo.domain.node.StoreEntity;
import org.alfresco.repo.domain.node.TransactionEntity;
import org.alfresco.repo.security.authentication.AuthenticationUtil;
import org.alfresco.service.cmr.repository.StoreRef;
import org.alfresco.util.ISO8601DateFormat;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Axel Faust
 */
public class NodeEntityBinarySerializerTests extends GridTestsBase
{

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeEntityBinarySerializerTests.class);

    protected static IgniteConfiguration createConfiguration(final boolean serialForm, final String... regionNames)
    {
        final IgniteConfiguration conf = createConfiguration(1, false, null);

        final BinaryConfiguration binaryConfiguration = new BinaryConfiguration();

        final BinaryTypeConfiguration binaryTypeConfigurationForNodeEntity = new BinaryTypeConfiguration();
        binaryTypeConfigurationForNodeEntity.setTypeName(NodeEntity.class.getName());
        final NodeEntityBinarySerializer serializer = new NodeEntityBinarySerializer();
        serializer.setUseRawSerialForm(serialForm);
        serializer.setUseVariableLengthIntegers(serialForm);
        binaryTypeConfigurationForNodeEntity.setSerializer(serializer);
        final BinaryTypeConfiguration binaryTypeConfigurationForNodeUpdateEntity = new BinaryTypeConfiguration();
        binaryTypeConfigurationForNodeUpdateEntity.setTypeName(NodeUpdateEntity.class.getName());
        binaryTypeConfigurationForNodeUpdateEntity.setSerializer(serializer);

        binaryConfiguration
                .setTypeConfigurations(Arrays.asList(binaryTypeConfigurationForNodeEntity, binaryTypeConfigurationForNodeUpdateEntity));
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

    @Rule
    public ExpectedException expected = ExpectedException.none();

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

            final CacheConfiguration<Long, NodeEntity> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("nodes");
            cacheConfig.setDataRegionName("comparison");
            final IgniteCache<Long, NodeEntity> referenceCache = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<Long, NodeEntity> cache = grid.getOrCreateCache(cacheConfig);

            // potential value deduplication (auditable values) and UUID optimisation - 14%
            this.efficiencyImpl(referenceGrid, grid, referenceCache, cache, "aldica optimised", "Ignite default", 0.14);
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

            final CacheConfiguration<Long, NodeEntity> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("nodes");
            cacheConfig.setDataRegionName("comparison");
            final IgniteCache<Long, NodeEntity> referenceCache = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<Long, NodeEntity> cache = grid.getOrCreateCache(cacheConfig);

            // variabel length integers and lack of field metadata in complex object - further 16 %
            this.efficiencyImpl(referenceGrid, grid, referenceCache, cache, "aldica raw serial", "aldica optimised", 0.16);
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
            final CacheConfiguration<Long, NodeEntity> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setName("nodes");
            cacheConfig.setCacheMode(CacheMode.LOCAL);
            final IgniteCache<Long, NodeEntity> cache = grid.getOrCreateCache(cacheConfig);

            NodeEntity controlValue;
            NodeEntity cacheValue;
            final StoreEntity storeControlValue;
            final TransactionEntity transactionControlValue;
            AuditablePropertiesEntity auditableControlValue;
            AuditablePropertiesEntity auditableCacheValue;

            // we don't really need different entities for store / transaction
            // their serialisation is tested in detail in other tests
            storeControlValue = new StoreEntity();
            storeControlValue.setId(1l);
            storeControlValue.setVersion(1l);
            storeControlValue.setProtocol(StoreRef.PROTOCOL_WORKSPACE);
            storeControlValue.setIdentifier("SpacesStore");

            transactionControlValue = new TransactionEntity();
            transactionControlValue.setId(1l);
            transactionControlValue.setVersion(1l);
            transactionControlValue.setChangeTxnId(UUID.randomUUID().toString());
            transactionControlValue.setCommitTimeMs(System.currentTimeMillis());

            // all set, uuid using expected format, different auditable values
            controlValue = new NodeEntity();
            controlValue.setId(1l);
            controlValue.setVersion(1l);
            controlValue.setStore(storeControlValue);
            controlValue.setUuid(UUID.randomUUID().toString());
            controlValue.setTypeQNameId(1l);
            controlValue.setLocaleId(1l);
            controlValue.setAclId(1l);
            controlValue.setTransaction(transactionControlValue);

            auditableControlValue = new AuditablePropertiesEntity();
            auditableControlValue.setAuditCreator(AuthenticationUtil.SYSTEM_USER_NAME);
            auditableControlValue.setAuditCreated("2020-01-01T00:00:00.000Z");
            auditableControlValue.setAuditModifier("admin");
            auditableControlValue.setAuditModified("2020-01-02T00:00:00.000Z");
            auditableControlValue.setAuditAccessed("2020-01-03T00:00:00.000Z");
            controlValue.setAuditableProperties(auditableControlValue);

            cache.put(1l, controlValue);
            cacheValue = cache.get(1l);
            auditableCacheValue = cacheValue.getAuditableProperties();

            // can't check for equals - value class does not support proper equals
            // check deep serialisation was actually involved (different value instances)
            // cannot check lock state directly
            Assert.assertNotSame(controlValue, cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getVersion(), cacheValue.getVersion());
            Assert.assertEquals(controlValue.getUuid(), cacheValue.getUuid());
            Assert.assertNotNull(cacheValue.getStore());
            Assert.assertNotSame(controlValue.getStore(), cacheValue.getStore());
            Assert.assertEquals(controlValue.getStore().getId(), cacheValue.getStore().getId());
            Assert.assertNotNull(cacheValue.getTransaction());
            Assert.assertNotSame(controlValue.getTransaction(), cacheValue.getTransaction());
            Assert.assertEquals(controlValue.getTransaction().getId(), cacheValue.getTransaction().getId());
            Assert.assertEquals(controlValue.getTypeQNameId(), cacheValue.getTypeQNameId());
            Assert.assertEquals(controlValue.getLocaleId(), cacheValue.getLocaleId());
            Assert.assertEquals(controlValue.getAclId(), cacheValue.getAclId());
            Assert.assertNotNull(auditableCacheValue);
            Assert.assertNotSame(auditableControlValue, auditableCacheValue);
            Assert.assertEquals(auditableControlValue.getAuditCreator(), auditableCacheValue.getAuditCreator());
            Assert.assertEquals(auditableControlValue.getAuditCreated(), auditableCacheValue.getAuditCreated());
            Assert.assertEquals(auditableControlValue.getAuditModifier(), auditableCacheValue.getAuditModifier());
            Assert.assertEquals(auditableControlValue.getAuditModified(), auditableCacheValue.getAuditModified());
            Assert.assertEquals(auditableControlValue.getAuditAccessed(), auditableCacheValue.getAuditAccessed());

            // minimal data, uuid using custom value, no audit values
            controlValue = new NodeEntity();
            controlValue.setId(2l);
            controlValue.setVersion(1l);
            controlValue.setStore(storeControlValue);
            controlValue.setUuid("my-custom-node-uuid");
            controlValue.setTypeQNameId(1l);
            controlValue.setLocaleId(1l);
            controlValue.setTransaction(transactionControlValue);

            auditableControlValue = new AuditablePropertiesEntity();
            controlValue.setAuditableProperties(auditableControlValue);

            cache.put(2l, controlValue);
            cacheValue = cache.get(2l);
            auditableCacheValue = cacheValue.getAuditableProperties();

            // can't check for equals - value class does not support proper equals
            // check deep serialisation was actually involved (different value instances)
            // cannot check lock state directly
            Assert.assertNotSame(controlValue, cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getVersion(), cacheValue.getVersion());
            Assert.assertEquals(controlValue.getUuid(), cacheValue.getUuid());
            Assert.assertNotNull(cacheValue.getStore());
            Assert.assertNotSame(controlValue.getStore(), cacheValue.getStore());
            Assert.assertEquals(controlValue.getStore().getId(), cacheValue.getStore().getId());
            Assert.assertNotNull(cacheValue.getTransaction());
            Assert.assertNotSame(controlValue.getTransaction(), cacheValue.getTransaction());
            Assert.assertEquals(controlValue.getTransaction().getId(), cacheValue.getTransaction().getId());
            Assert.assertEquals(controlValue.getTypeQNameId(), cacheValue.getTypeQNameId());
            Assert.assertEquals(controlValue.getLocaleId(), cacheValue.getLocaleId());
            Assert.assertEquals(controlValue.getAclId(), cacheValue.getAclId());
            Assert.assertNotNull(auditableCacheValue);
            Assert.assertNotSame(auditableControlValue, auditableCacheValue);
            Assert.assertEquals(auditableControlValue.getAuditCreator(), auditableCacheValue.getAuditCreator());
            Assert.assertEquals(auditableControlValue.getAuditCreated(), auditableCacheValue.getAuditCreated());
            Assert.assertEquals(auditableControlValue.getAuditModifier(), auditableCacheValue.getAuditModifier());
            Assert.assertEquals(auditableControlValue.getAuditModified(), auditableCacheValue.getAuditModified());
            Assert.assertEquals(auditableControlValue.getAuditAccessed(), auditableCacheValue.getAuditAccessed());

            // identical audit values
            controlValue = new NodeEntity();
            controlValue.setId(3l);
            controlValue.setVersion(1l);
            controlValue.setStore(storeControlValue);
            controlValue.setUuid(UUID.randomUUID().toString());
            controlValue.setTypeQNameId(1l);
            controlValue.setLocaleId(1l);
            controlValue.setTransaction(transactionControlValue);

            auditableControlValue = new AuditablePropertiesEntity();
            auditableControlValue.setAuditCreator(AuthenticationUtil.SYSTEM_USER_NAME);
            auditableControlValue.setAuditCreated("2020-01-01T00:00:00.000Z");
            auditableControlValue.setAuditModifier(AuthenticationUtil.SYSTEM_USER_NAME);
            auditableControlValue.setAuditModified("2020-01-01T00:00:00.000Z");
            controlValue.setAuditableProperties(auditableControlValue);

            cache.put(3l, controlValue);
            cacheValue = cache.get(3l);
            auditableCacheValue = cacheValue.getAuditableProperties();

            // can't check for equals - value class does not support proper equals
            // check deep serialisation was actually involved (different value instances)
            // cannot check lock state directly
            Assert.assertNotSame(controlValue, cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getVersion(), cacheValue.getVersion());
            Assert.assertEquals(controlValue.getUuid(), cacheValue.getUuid());
            Assert.assertNotNull(cacheValue.getStore());
            Assert.assertNotSame(controlValue.getStore(), cacheValue.getStore());
            Assert.assertEquals(controlValue.getStore().getId(), cacheValue.getStore().getId());
            Assert.assertNotNull(cacheValue.getTransaction());
            Assert.assertNotSame(controlValue.getTransaction(), cacheValue.getTransaction());
            Assert.assertEquals(controlValue.getTransaction().getId(), cacheValue.getTransaction().getId());
            Assert.assertEquals(controlValue.getTypeQNameId(), cacheValue.getTypeQNameId());
            Assert.assertEquals(controlValue.getLocaleId(), cacheValue.getLocaleId());
            Assert.assertEquals(controlValue.getAclId(), cacheValue.getAclId());
            Assert.assertNotNull(auditableCacheValue);
            Assert.assertNotSame(auditableControlValue, auditableCacheValue);
            Assert.assertEquals(auditableControlValue.getAuditCreator(), auditableCacheValue.getAuditCreator());
            Assert.assertEquals(auditableControlValue.getAuditCreated(), auditableCacheValue.getAuditCreated());
            Assert.assertEquals(auditableControlValue.getAuditModifier(), auditableCacheValue.getAuditModifier());
            Assert.assertEquals(auditableControlValue.getAuditModified(), auditableCacheValue.getAuditModified());
            Assert.assertEquals(auditableControlValue.getAuditAccessed(), auditableCacheValue.getAuditAccessed());

            // check update entity is supported, though update flags are ignored
            controlValue = new NodeUpdateEntity();
            controlValue.setId(4l);
            controlValue.setVersion(1l);
            controlValue.setStore(storeControlValue);
            controlValue.setUuid(UUID.randomUUID().toString());
            controlValue.setTypeQNameId(1l);
            controlValue.setLocaleId(1l);
            controlValue.setTransaction(transactionControlValue);

            ((NodeUpdateEntity) controlValue).setUpdateTypeQNameId(true);
            ((NodeUpdateEntity) controlValue).setUpdateLocaleId(true);
            ((NodeUpdateEntity) controlValue).setUpdateAclId(true);
            ((NodeUpdateEntity) controlValue).setUpdateTransaction(true);
            ((NodeUpdateEntity) controlValue).setUpdateAuditableProperties(true);

            auditableControlValue = new AuditablePropertiesEntity();
            auditableControlValue.setAuditCreator(AuthenticationUtil.SYSTEM_USER_NAME);
            auditableControlValue.setAuditCreated("2020-01-01T00:00:00.000Z");
            auditableControlValue.setAuditModifier("admin");
            auditableControlValue.setAuditModified("2020-01-02T00:00:00.000Z");
            controlValue.setAuditableProperties(auditableControlValue);

            cache.put(4l, controlValue);
            cacheValue = cache.get(4l);
            auditableCacheValue = cacheValue.getAuditableProperties();

            // can't check for equals - value class does not support proper equals
            // check deep serialisation was actually involved (different value instances)
            // cannot check lock state directly
            Assert.assertNotSame(controlValue, cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getVersion(), cacheValue.getVersion());
            Assert.assertEquals(controlValue.getUuid(), cacheValue.getUuid());
            Assert.assertNotNull(cacheValue.getStore());
            Assert.assertNotSame(controlValue.getStore(), cacheValue.getStore());
            Assert.assertEquals(controlValue.getStore().getId(), cacheValue.getStore().getId());
            Assert.assertNotNull(cacheValue.getTransaction());
            Assert.assertNotSame(controlValue.getTransaction(), cacheValue.getTransaction());
            Assert.assertEquals(controlValue.getTransaction().getId(), cacheValue.getTransaction().getId());
            Assert.assertEquals(controlValue.getTypeQNameId(), cacheValue.getTypeQNameId());
            Assert.assertEquals(controlValue.getLocaleId(), cacheValue.getLocaleId());
            Assert.assertEquals(controlValue.getAclId(), cacheValue.getAclId());
            Assert.assertNotNull(auditableCacheValue);
            Assert.assertNotSame(auditableControlValue, auditableCacheValue);
            Assert.assertEquals(auditableControlValue.getAuditCreator(), auditableCacheValue.getAuditCreator());
            Assert.assertEquals(auditableControlValue.getAuditCreated(), auditableCacheValue.getAuditCreated());
            Assert.assertEquals(auditableControlValue.getAuditModifier(), auditableCacheValue.getAuditModifier());
            Assert.assertEquals(auditableControlValue.getAuditModified(), auditableCacheValue.getAuditModified());
            Assert.assertEquals(auditableControlValue.getAuditAccessed(), auditableCacheValue.getAuditAccessed());

            Assert.assertFalse(((NodeUpdateEntity) cacheValue).isUpdateTypeQNameId());
            Assert.assertFalse(((NodeUpdateEntity) cacheValue).isUpdateLocaleId());
            Assert.assertFalse(((NodeUpdateEntity) cacheValue).isUpdateAclId());
            Assert.assertFalse(((NodeUpdateEntity) cacheValue).isUpdateTransaction());
            Assert.assertFalse(((NodeUpdateEntity) cacheValue).isUpdateAuditableProperties());

            // check locked flag indirectly via modification
            this.expected.expect(IllegalStateException.class);
            cacheValue.setId(-1l);
        }
    }

    protected void efficiencyImpl(final Ignite referenceGrid, final Ignite grid, final IgniteCache<Long, NodeEntity> referenceCache,
            final IgniteCache<Long, NodeEntity> cache, final String serialisationType, final String referenceSerialisationType,
            final double marginFraction)
    {
        LOGGER.info(
                "Running NodeEntity serialisation benchmark of 100k instances, comparing {} vs. {} serialisation, expecting relative improvement margin / difference fraction of {}",
                referenceSerialisationType, serialisationType, marginFraction);

        final StoreEntity storeValue;
        final TransactionEntity transactionValue;

        // we don't really need different entities for store / transaction
        // their serialisation is benchmarked in other tests
        storeValue = new StoreEntity();
        storeValue.setId(1l);
        storeValue.setVersion(1l);
        storeValue.setProtocol(StoreRef.PROTOCOL_WORKSPACE);
        storeValue.setIdentifier("SpacesStore");

        transactionValue = new TransactionEntity();
        transactionValue.setId(1l);
        transactionValue.setVersion(1l);
        transactionValue.setChangeTxnId(UUID.randomUUID().toString());
        transactionValue.setCommitTimeMs(System.currentTimeMillis());

        final SecureRandom rnJesus = new SecureRandom();

        for (int idx = 0; idx < 100000; idx++)
        {
            final NodeEntity value = new NodeEntity();
            value.setId(Long.valueOf(idx));
            value.setVersion(Long.valueOf(rnJesus.nextInt(1024)));
            value.setStore(storeValue);
            value.setUuid(UUID.randomUUID().toString());
            // systems with extensive metadata can have many QName values
            value.setTypeQNameId(Long.valueOf(rnJesus.nextInt(2048)));
            // very few locales in a system
            value.setLocaleId(Long.valueOf(rnJesus.nextInt(64)));
            // not ever node has its own ACL, so this simulates potential reuse of IDs
            value.setAclId(Long.valueOf(rnJesus.nextInt(idx + 1)));
            value.setTransaction(transactionValue);

            // random technical user IDs - creation / modification in a year's range - 25% chance to have identical values
            final AuditablePropertiesEntity auditableValue = new AuditablePropertiesEntity();
            value.setAuditableProperties(auditableValue);

            final Instant created = Instant.now().minusMillis(1000l * rnJesus.nextInt(31536000));
            final String creator = "u" + rnJesus.nextInt(10000);
            auditableValue.setAuditCreator(creator);
            auditableValue.setAuditCreated(ISO8601DateFormat.format(Date.from(created)));

            if (rnJesus.nextDouble() >= 0.75)
            {
                auditableValue.setAuditModifier(creator);
                auditableValue.setAuditModified(ISO8601DateFormat.format(Date.from(created)));
            }
            else
            {
                final Instant modified = created.plusMillis(1000l * rnJesus.nextInt(31536000));
                auditableValue.setAuditModifier("u" + rnJesus.nextInt(10000));
                auditableValue.setAuditModified(ISO8601DateFormat.format(Date.from(modified)));
            }

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
