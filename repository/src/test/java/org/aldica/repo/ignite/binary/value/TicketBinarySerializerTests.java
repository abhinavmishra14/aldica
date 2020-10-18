/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
package org.aldica.repo.ignite.binary.value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.aldica.common.ignite.GridTestsBase;
import org.aldica.repo.ignite.ExpensiveTestCategory;
import org.aldica.repo.ignite.binary.value.TicketBinarySerializer;
import org.aldica.repo.ignite.cache.SimpleIgniteBackedCache;
import org.aldica.repo.ignite.cache.SimpleIgniteBackedCache.Mode;
import org.aldica.repo.ignite.util.CapturingCacheFacade;
import org.alfresco.repo.security.authentication.InMemoryTicketComponentImpl;
import org.alfresco.repo.security.authentication.InMemoryTicketComponentImpl.ExpiryMode;
import org.alfresco.repo.security.authentication.InMemoryTicketComponentImpl.Ticket;
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
public class TicketBinarySerializerTests extends GridTestsBase
{

    private static final Logger LOGGER = LoggerFactory.getLogger(TicketBinarySerializerTests.class);

    protected static IgniteConfiguration createConfiguration(final boolean serialForm, final String... regionNames)
    {
        final IgniteConfiguration conf = createConfiguration(1, false, null);

        final BinaryConfiguration binaryConfiguration = new BinaryConfiguration();

        final BinaryTypeConfiguration binaryTypeConfigurationForTicket = new BinaryTypeConfiguration();
        binaryTypeConfigurationForTicket.setTypeName(Ticket.class.getName());
        final TicketBinarySerializer serializer = new TicketBinarySerializer();
        serializer.setUseRawSerialForm(serialForm);
        serializer.setUseVariableLengthIntegers(serialForm);
        binaryTypeConfigurationForTicket.setSerializer(serializer);

        binaryConfiguration.setTypeConfigurations(Arrays.asList(binaryTypeConfigurationForTicket));
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
        final IgniteConfiguration conf = createConfiguration(false, "comparison");

        referenceConf.setDataStorageConfiguration(conf.getDataStorageConfiguration());

        try
        {
            final Ignite referenceGrid = Ignition.start(referenceConf);
            final Ignite grid = Ignition.start(conf);

            final CacheConfiguration<String, Ticket> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("tickets");
            cacheConfig.setDataRegionName("comparison");
            final IgniteCache<String, Ticket> referenceCache = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<String, Ticket> cache = grid.getOrCreateCache(cacheConfig);

            // no differences - possibly negligible overhead
            this.efficiencyImpl(referenceGrid, grid, referenceCache, cache, "aldica optimised", "Ignite default", -0.002);
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

            final CacheConfiguration<String, Ticket> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("tickets");
            cacheConfig.setDataRegionName("comparison");
            final IgniteCache<String, Ticket> referenceCache1 = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<String, Ticket> cache1 = grid.getOrCreateCache(cacheConfig);

            // small differences due to variable length integers - 6%
            this.efficiencyImpl(referenceGrid, grid, referenceCache1, cache1, "aldica raw serial", "aldica optimised", 0.06);
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
            final CacheConfiguration<String, Ticket> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setName("tickets");
            cacheConfig.setCacheMode(CacheMode.LOCAL);
            final IgniteCache<String, Ticket> cache = grid.getOrCreateCache(cacheConfig);

            final InMemoryTicketComponentImpl ticketComponent = new InMemoryTicketComponentImpl();
            ticketComponent.setExpiryMode(ExpiryMode.AFTER_INACTIVITY.name());
            // redundant but required
            ticketComponent.setTicketsExpire(true);
            ticketComponent.setValidDuration("P1H");
            ticketComponent.setUseSingleTicketPerUser(true);
            final CapturingCacheFacade<String, Ticket> cacheFacade = new CapturingCacheFacade<>(
                    new SimpleIgniteBackedCache<>(grid, Mode.LOCAL, cache, false));
            ticketComponent.setTicketsCache(cacheFacade);

            Ticket controlValue;
            Ticket cacheValue;

            ticketComponent.getNewTicket("admin");
            controlValue = cacheFacade.getLastValue();

            ticketComponent.getCurrentTicket("admin", false);
            cacheValue = cacheFacade.getLastValue();

            Assert.assertEquals(controlValue, cacheValue);
            Assert.assertNotSame(controlValue, cacheValue);

            ticketComponent.setExpiryMode(ExpiryMode.AFTER_FIXED_TIME.name());
            ticketComponent.setValidDuration("P10H");

            ticketComponent.getNewTicket("guest");
            controlValue = cacheFacade.getLastValue();

            ticketComponent.getCurrentTicket("guest", false);
            cacheValue = cacheFacade.getLastValue();

            Assert.assertEquals(controlValue, cacheValue);
            Assert.assertNotSame(controlValue, cacheValue);

            // ExpiryMode.DO_NOT_EXPIRE is bugged in Alfresco => NPE in Ticket.equals
        }
    }

    protected void efficiencyImpl(final Ignite referenceGrid, final Ignite grid, final IgniteCache<String, Ticket> referenceCache,
            final IgniteCache<String, Ticket> cache, final String serialisationType, final String referenceSerialisationType,
            final double marginFraction)
    {
        LOGGER.info(
                "Running Ticket serialisation benchmark of 5k instances, comparing {} vs. {} serialisation, expecting relative improvement margin / difference fraction of {}",
                referenceSerialisationType, serialisationType, marginFraction);

        final InMemoryTicketComponentImpl tc = new InMemoryTicketComponentImpl();
        tc.setExpiryMode(ExpiryMode.AFTER_INACTIVITY.name());
        // redundant but required
        tc.setTicketsExpire(true);
        tc.setValidDuration("P1H");
        tc.setUseSingleTicketPerUser(true);
        tc.setTicketsCache(new SimpleIgniteBackedCache<>(grid, Mode.LOCAL, cache, false));

        final InMemoryTicketComponentImpl referenceTc = new InMemoryTicketComponentImpl();
        referenceTc.setExpiryMode(ExpiryMode.AFTER_INACTIVITY.name());
        // redundant but required
        referenceTc.setTicketsExpire(true);
        referenceTc.setValidDuration("P1H");
        referenceTc.setUseSingleTicketPerUser(true);
        referenceTc.setTicketsCache(new SimpleIgniteBackedCache<>(referenceGrid, Mode.LOCAL, referenceCache, false));

        // only 5k instances as opposed to 100k for other tests
        // TicketComponent handling is way more expensive duration-wise
        for (int idx = 0; idx < 5000; idx++)
        {
            final String pseudoUser = UUID.randomUUID().toString();
            tc.getNewTicket(pseudoUser);
            referenceTc.getNewTicket(pseudoUser);
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
