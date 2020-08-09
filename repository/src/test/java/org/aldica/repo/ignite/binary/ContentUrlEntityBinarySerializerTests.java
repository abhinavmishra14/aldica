/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
package org.aldica.repo.ignite.binary;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.aldica.common.ignite.GridTestsBase;
import org.aldica.repo.ignite.ExpensiveTestCategory;
import org.alfresco.repo.content.filestore.FileContentStore;
import org.alfresco.repo.content.filestore.FileContentUrlProvider;
import org.alfresco.repo.domain.contentdata.ContentUrlEntity;
import org.alfresco.repo.domain.contentdata.ContentUrlKeyEntity;
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
public class ContentUrlEntityBinarySerializerTests extends GridTestsBase
{

    private static final Logger LOGGER = LoggerFactory.getLogger(ContentUrlEntityBinarySerializerTests.class);

    protected static IgniteConfiguration createConfiguration(final boolean serialForm, final String... regionNames)
    {
        final IgniteConfiguration conf = createConfiguration(1, false, null);

        final BinaryConfiguration binaryConfiguration = new BinaryConfiguration();

        final BinaryTypeConfiguration binaryTypeConfigurationForStoreRef = new BinaryTypeConfiguration();
        binaryTypeConfigurationForStoreRef.setTypeName(ContentUrlEntity.class.getName());
        final ContentUrlEntityBinarySerializer serializer = new ContentUrlEntityBinarySerializer();
        serializer.setUseRawSerialForm(serialForm);
        serializer.setUseVariableLengthIntegers(serialForm);
        serializer.setUseOptimisedContentURL(serialForm);
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
        final IgniteConfiguration conf = createConfiguration(false, "comparisonWithoutKeys", "comparisonWithKeys");

        referenceConf.setDataStorageConfiguration(conf.getDataStorageConfiguration());

        try
        {
            final Ignite referenceGrid = Ignition.start(referenceConf);
            final Ignite grid = Ignition.start(conf);

            final CacheConfiguration<Long, ContentUrlEntity> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("contentUrlsWithoutKeys");
            cacheConfig.setDataRegionName("comparisonWithoutKeys");
            final IgniteCache<Long, ContentUrlEntity> referenceCacheWoKeys = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<Long, ContentUrlEntity> cacheWoKeys = grid.getOrCreateCache(cacheConfig);

            cacheConfig.setName("contentUrlsWithKeys");
            cacheConfig.setDataRegionName("comparisonWithKeys");
            final IgniteCache<Long, ContentUrlEntity> referenceCacheWKeys = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<Long, ContentUrlEntity> cacheWKeys = grid.getOrCreateCache(cacheConfig);

            // we avoid having secondary fields in the serial form, so quite a bit of difference - 12%
            this.efficiencyImpl(referenceGrid, grid, referenceCacheWoKeys, cacheWoKeys, "aldica optimised", "Ignite default", false, 0.12);

            // savings are negligible due to high cost of key payload
            this.efficiencyImpl(referenceGrid, grid, referenceCacheWKeys, cacheWKeys, "aldica optimised", "Ignite default", true, 0);
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
        final IgniteConfiguration referenceConf = createConfiguration(false, "comparisonWithoutKeys", "comparisonWithKeys");
        referenceConf.setIgniteInstanceName(referenceConf.getIgniteInstanceName() + "-reference");
        final IgniteConfiguration conf = createConfiguration(true, "comparisonWithoutKeys", "comparisonWithKeys");

        try
        {
            final Ignite referenceGrid = Ignition.start(referenceConf);
            final Ignite grid = Ignition.start(conf);

            final CacheConfiguration<Long, ContentUrlEntity> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("contentUrlsWithoutKeys");
            cacheConfig.setDataRegionName("comparisonWithoutKeys");
            final IgniteCache<Long, ContentUrlEntity> referenceCacheWoKeys = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<Long, ContentUrlEntity> cacheWoKeys = grid.getOrCreateCache(cacheConfig);

            cacheConfig.setName("contentUrlsWithKeys");
            cacheConfig.setDataRegionName("comparisonWithKeys");
            final IgniteCache<Long, ContentUrlEntity> referenceCacheWKeys = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<Long, ContentUrlEntity> cacheWKeys = grid.getOrCreateCache(cacheConfig);

            // we optimise serial form for most value components, so quite a bit of difference - 19%
            this.efficiencyImpl(referenceGrid, grid, referenceCacheWoKeys, cacheWoKeys, "aldica raw serial", "aldica optimised", false,
                    0.19);

            // savings are negligible due to high cost of key payload
            this.efficiencyImpl(referenceGrid, grid, referenceCacheWKeys, cacheWKeys, "aldica raw serial", "aldica optimised", true, 0);
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
            final CacheConfiguration<Long, ContentUrlEntity> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setName("contentUrls");
            cacheConfig.setCacheMode(CacheMode.LOCAL);
            final IgniteCache<Long, ContentUrlEntity> cache = grid.getOrCreateCache(cacheConfig);

            // default Alfresco classes are inaccessible (package-protected visibility)
            final FileContentUrlProvider urlProvider = () -> FileContentStore.STORE_PROTOCOL + "://" + UUID.randomUUID().toString();

            ContentUrlEntity controlValue;
            ContentUrlEntity cacheValue;
            ContentUrlKeyEntity keyControlValue;
            ContentUrlKeyEntity keyCacheValue;

            // most common case - unorphaned, unencrypted
            controlValue = new ContentUrlEntity();
            controlValue.setId(1l);
            controlValue.setContentUrl(urlProvider.createNewFileStoreUrl());
            controlValue.setSize(123l);
            controlValue.setOrphanTime(null);

            cache.put(1l, controlValue);
            cacheValue = cache.get(1l);

            Assert.assertEquals(controlValue, cacheValue);
            // check deep serialisation was actually involved (different value instances)
            Assert.assertFalse(controlValue == cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getContentUrlShort(), cacheValue.getContentUrlShort());
            Assert.assertEquals(controlValue.getContentUrlCrc(), cacheValue.getContentUrlCrc());
            Assert.assertEquals(controlValue.getSize(), cacheValue.getSize());
            Assert.assertNull(cacheValue.getOrphanTime());
            Assert.assertNull(cacheValue.getContentUrlKey());

            // second most common case - orphaned, unencrypted
            controlValue = new ContentUrlEntity();
            controlValue.setId(2l);
            controlValue.setContentUrl(urlProvider.createNewFileStoreUrl());
            controlValue.setSize(321l);
            controlValue.setOrphanTime(System.currentTimeMillis());

            cache.put(2l, controlValue);
            cacheValue = cache.get(2l);

            Assert.assertEquals(controlValue, cacheValue);
            // check deep serialisation was actually involved (different value instances)
            Assert.assertFalse(controlValue == cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getContentUrlShort(), cacheValue.getContentUrlShort());
            Assert.assertEquals(controlValue.getContentUrlCrc(), cacheValue.getContentUrlCrc());
            Assert.assertEquals(controlValue.getSize(), cacheValue.getSize());
            Assert.assertEquals(controlValue.getOrphanTime(), cacheValue.getOrphanTime());
            Assert.assertNull(cacheValue.getContentUrlKey());

            // not sure when this case ever exists in Alfresco, but legally possible
            controlValue = new ContentUrlEntity();
            controlValue.setId(3l);
            // controlValue.setContentUrl(null); // null is default value and setter is not null-safe
            controlValue.setSize(999l);
            controlValue.setOrphanTime(null);

            cache.put(3l, controlValue);
            cacheValue = cache.get(3l);

            Assert.assertEquals(controlValue, cacheValue);
            // check deep serialisation was actually involved (different value instances)
            Assert.assertFalse(controlValue == cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertNull(cacheValue.getContentUrlShort());
            Assert.assertEquals(controlValue.getContentUrlCrc(), cacheValue.getContentUrlCrc());
            Assert.assertEquals(controlValue.getSize(), cacheValue.getSize());
            Assert.assertNull(cacheValue.getOrphanTime());
            Assert.assertNull(cacheValue.getContentUrlKey());

            // case which normally only exists for Enterprise, unorphaned, encrypted
            controlValue = new ContentUrlEntity();
            controlValue.setId(4l);
            controlValue.setContentUrl(urlProvider.createNewFileStoreUrl());
            controlValue.setSize(123456789l);
            controlValue.setOrphanTime(null);
            keyControlValue = new ContentUrlKeyEntity();
            keyControlValue.setId(1l);
            keyControlValue.setContentUrlId(4l);
            keyControlValue.setEncryptedKeyAsBytes(new byte[] { 123, -123, 15, -15, 64, -64, 0, 1, -1 });
            keyControlValue.setKeySize(1024);
            keyControlValue.setAlgorithm("MyTestAlgorithm");
            keyControlValue.setMasterKeystoreId("masterKeystore");
            keyControlValue.setMasterKeyAlias("masterAlias");
            keyControlValue.setUnencryptedFileSize(123456l);
            controlValue.setContentUrlKey(keyControlValue);

            cache.put(4l, controlValue);
            cacheValue = cache.get(4l);
            keyCacheValue = cacheValue.getContentUrlKey();

            Assert.assertEquals(controlValue, cacheValue);
            // check deep serialisation was actually involved (different value instances)
            Assert.assertFalse(controlValue == cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getContentUrlShort(), cacheValue.getContentUrlShort());
            Assert.assertEquals(controlValue.getContentUrlCrc(), cacheValue.getContentUrlCrc());
            Assert.assertEquals(controlValue.getSize(), cacheValue.getSize());
            Assert.assertNull(cacheValue.getOrphanTime());

            Assert.assertEquals(keyControlValue, keyCacheValue);
            Assert.assertFalse(keyControlValue == keyCacheValue);
            // equals checks most, but not all
            Assert.assertEquals(keyControlValue.getContentUrlId(), keyCacheValue.getContentUrlId());
            Assert.assertTrue(Arrays.equals(keyControlValue.getEncryptedKeyAsBytes(), keyCacheValue.getEncryptedKeyAsBytes()));
            Assert.assertEquals(keyControlValue.getKeySize(), keyCacheValue.getKeySize());
            Assert.assertEquals(keyControlValue.getUnencryptedFileSize(), keyCacheValue.getUnencryptedFileSize());

            // case with non-default content URL - unorphaned, unencrypted
            controlValue = new ContentUrlEntity();
            controlValue.setId(5l);
            controlValue.setContentUrl("my-store://path/to/file/with/weird.name");
            controlValue.setSize(987654321l);
            controlValue.setOrphanTime(null);

            cache.put(5l, controlValue);
            cacheValue = cache.get(5l);

            Assert.assertEquals(controlValue, cacheValue);
            // check deep serialisation was actually involved (different value instances)
            Assert.assertFalse(controlValue == cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertEquals(controlValue.getContentUrlShort(), cacheValue.getContentUrlShort());
            Assert.assertEquals(controlValue.getContentUrlCrc(), cacheValue.getContentUrlCrc());
            Assert.assertEquals(controlValue.getSize(), cacheValue.getSize());
            Assert.assertNull(cacheValue.getOrphanTime());
            Assert.assertNull(cacheValue.getContentUrlKey());

            // weird case in default Alfresco where ID is not set despite being persisted in the DB
            controlValue = new ContentUrlEntity();
            controlValue.setId(null);
            // controlValue.setContentUrl(null); // null is default value and setter is not null-safe
            controlValue.setSize(999l);
            controlValue.setOrphanTime(null);

            cache.put(6l, controlValue);
            cacheValue = cache.get(6l);

            Assert.assertEquals(controlValue, cacheValue);
            // check deep serialisation was actually involved (different value instances)
            Assert.assertFalse(controlValue == cacheValue);
            Assert.assertEquals(controlValue.getId(), cacheValue.getId());
            Assert.assertNull(cacheValue.getContentUrlShort());
            Assert.assertEquals(controlValue.getContentUrlCrc(), cacheValue.getContentUrlCrc());
            Assert.assertEquals(controlValue.getSize(), cacheValue.getSize());
            Assert.assertNull(cacheValue.getOrphanTime());
            Assert.assertNull(cacheValue.getContentUrlKey());
        }
    }

    protected void efficiencyImpl(final Ignite referenceGrid, final Ignite grid, final IgniteCache<Long, ContentUrlEntity> referenceCache,
            final IgniteCache<Long, ContentUrlEntity> cache, final String serialisationType, final String referenceSerialisationType,
            final boolean withKeys, final double marginFraction)
    {
        LOGGER.info(
                "Running ContentUrlEntity serialisation benchmark of 100k instances (withKeys = {}), comparing {} vs. {} serialisation, expecting relative improvement margin / difference fraction of {}",
                withKeys, referenceSerialisationType, serialisationType, marginFraction);

        // default Alfresco classes are inaccessible (package-protected visibility)
        final FileContentUrlProvider urlProvider = () -> FileContentStore.STORE_PROTOCOL + "://" + UUID.randomUUID().toString();

        final SecureRandom rnJesus = new SecureRandom();
        final long now = System.currentTimeMillis();

        for (int idx = 0; idx < 100000; idx++)
        {
            final ContentUrlEntity value = new ContentUrlEntity();
            value.setId(Long.valueOf(idx));
            value.setContentUrl(urlProvider.createNewFileStoreUrl());
            value.setSize(10240l + rnJesus.nextInt(Integer.MAX_VALUE));
            value.setOrphanTime(now + rnJesus.nextInt(365 * 24 * 60 * 60 * 1000));

            if (withKeys)
            {
                final ContentUrlKeyEntity key = new ContentUrlKeyEntity();
                key.setId(value.getId());
                key.setContentUrlId(value.getId());

                final byte[] keyBytes = new byte[1024];
                rnJesus.nextBytes(keyBytes);
                key.setEncryptedKeyAsBytes(keyBytes);
                key.setKeySize(keyBytes.length);

                key.setAlgorithm("SHA0815");
                key.setMasterKeystoreId("masterKeystore");
                key.setMasterKeyAlias("masterKey");
                key.setUnencryptedFileSize(value.getSize() - rnJesus.nextInt(10240));

                value.setContentUrlKey(key);
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
