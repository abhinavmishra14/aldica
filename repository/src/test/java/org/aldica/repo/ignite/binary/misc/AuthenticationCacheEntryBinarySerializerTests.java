/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
package org.aldica.repo.ignite.binary.misc;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.aldica.common.ignite.GridTestsBase;
import org.aldica.repo.ignite.ExpensiveTestCategory;
import org.alfresco.error.AlfrescoRuntimeException;
import org.alfresco.repo.security.authentication.CompositePasswordEncoder;
import org.alfresco.repo.security.authentication.MD4PasswordEncoderImpl;
import org.alfresco.repo.security.authentication.RepositoryAuthenticatedUser;
import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.cmr.repository.StoreRef;
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

import net.sf.acegisecurity.GrantedAuthority;
import net.sf.acegisecurity.GrantedAuthorityImpl;
import net.sf.acegisecurity.UserDetails;
import net.sf.acegisecurity.providers.encoding.PasswordEncoder;

/**
 * @author Axel Faust
 */
public class AuthenticationCacheEntryBinarySerializerTests extends GridTestsBase
{

    private static final Class<?> CACHE_ENTRY_CLASS;

    private static final Constructor<?> CACHE_ENTRY_CTOR;

    private static final Field CACHE_ENTRY_NODE_REF;

    private static final Field CACHE_ENTRY_USER_DETAILS;

    private static final Field CACHE_ENTRY_CREDENTIALS_EXPIRY_DATE;

    static
    {
        try
        {
            CACHE_ENTRY_CLASS = Class.forName("org.alfresco.repo.security.authentication.RepositoryAuthenticationDao$CacheEntry");
            CACHE_ENTRY_CTOR = CACHE_ENTRY_CLASS.getDeclaredConstructor(NodeRef.class, UserDetails.class, Date.class);
            CACHE_ENTRY_NODE_REF = CACHE_ENTRY_CLASS.getDeclaredField("nodeRef");
            CACHE_ENTRY_USER_DETAILS = CACHE_ENTRY_CLASS.getDeclaredField("userDetails");
            CACHE_ENTRY_CREDENTIALS_EXPIRY_DATE = CACHE_ENTRY_CLASS.getDeclaredField("credentialExpiryDate");

            CACHE_ENTRY_CTOR.setAccessible(true);
            CACHE_ENTRY_NODE_REF.setAccessible(true);
            CACHE_ENTRY_USER_DETAILS.setAccessible(true);
            CACHE_ENTRY_CREDENTIALS_EXPIRY_DATE.setAccessible(true);
        }
        catch (ClassNotFoundException | NoSuchMethodException | NoSuchFieldException e)
        {
            throw new AlfrescoRuntimeException("Failed to lookup class / constructor / fields");
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthenticationCacheEntryBinarySerializerTests.class);

    protected static IgniteConfiguration createConfiguration(final boolean serialForm, final String... regionNames)
    {
        final IgniteConfiguration conf = createConfiguration(1, false, null);

        final BinaryConfiguration binaryConfiguration = new BinaryConfiguration();

        final BinaryTypeConfiguration binaryTypeConfigurationForCacheEntry = new BinaryTypeConfiguration();
        binaryTypeConfigurationForCacheEntry.setTypeName(CACHE_ENTRY_CLASS.getName());
        final AuthenticationCacheEntryBinarySerializer serializer = new AuthenticationCacheEntryBinarySerializer();
        serializer.setUseRawSerialForm(serialForm);
        serializer.setUseVariableLengthIntegers(serialForm);
        binaryTypeConfigurationForCacheEntry.setSerializer(serializer);

        binaryConfiguration.setTypeConfigurations(Arrays.asList(binaryTypeConfigurationForCacheEntry));
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
        final IgniteConfiguration conf = createConfiguration(false, "comparison");

        referenceConf.setDataStorageConfiguration(conf.getDataStorageConfiguration());

        try
        {
            final Ignite referenceGrid = Ignition.start(referenceConf);
            final Ignite grid = Ignition.start(conf);

            final CacheConfiguration<String, Object> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("authenticationCacheEntries");
            cacheConfig.setDataRegionName("comparison");
            final IgniteCache<String, Object> referenceCache = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<String, Object> cache = grid.getOrCreateCache(cacheConfig);

            // inlining and flag aggregation - 10%
            this.efficiencyImpl(referenceGrid, grid, referenceCache, cache, "aldica optimised", "Ignite default", 0.1);
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
        final IgniteConfiguration referenceConf = createConfiguration(false, "comparison");
        referenceConf.setIgniteInstanceName(referenceConf.getIgniteInstanceName() + "-reference");
        final IgniteConfiguration conf = createConfiguration(true, "comparison");

        try
        {
            final Ignite referenceGrid = Ignition.start(referenceConf);
            final Ignite grid = Ignition.start(conf);

            final CacheConfiguration<String, Object> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setCacheMode(CacheMode.LOCAL);

            cacheConfig.setName("authenticationCacheEntries");
            cacheConfig.setDataRegionName("comparison");
            final IgniteCache<String, Object> referenceCache1 = referenceGrid.getOrCreateCache(cacheConfig);
            final IgniteCache<String, Object> cache1 = grid.getOrCreateCache(cacheConfig);

            // optimised string / variable integers - 9%
            this.efficiencyImpl(referenceGrid, grid, referenceCache1, cache1, "aldica raw serial", "aldica optimised", 0.09);
        }
        finally
        {
            Ignition.stopAll(true);
        }
    }

    protected void correctnessImpl(final IgniteConfiguration conf)
            throws InstantiationException, InvocationTargetException, IllegalAccessException
    {
        try (Ignite grid = Ignition.start(conf))
        {
            final CacheConfiguration<String, Object> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setName("authenticationCacheEntries");
            cacheConfig.setCacheMode(CacheMode.LOCAL);
            final IgniteCache<String, Object> cache = grid.getOrCreateCache(cacheConfig);

            Object controlValue;
            NodeRef controlNode;
            RepositoryAuthenticatedUser controlUser;
            Date controlExpiryDate;

            Object cacheValue;
            NodeRef cacheNode;
            RepositoryAuthenticatedUser cacheUser;
            Date cacheExpiryDate;

            controlNode = new NodeRef(StoreRef.STORE_REF_WORKSPACE_SPACESSTORE, UUID.randomUUID().toString());
            controlUser = new RepositoryAuthenticatedUser("jdoe", "someHash", true, true, true, true,
                    new GrantedAuthority[] { new GrantedAuthorityImpl("role1"), new GrantedAuthorityImpl("role2") },
                    CompositePasswordEncoder.MD4, UUID.randomUUID());
            controlExpiryDate = Date.from(Instant.now().plus(5, ChronoUnit.DAYS));

            controlValue = CACHE_ENTRY_CTOR.newInstance(controlNode, controlUser, controlExpiryDate);
            cache.put("jdoe", controlValue);
            cacheValue = cache.get("jdoe");

            Assert.assertNotSame(controlValue, cacheValue);

            cacheNode = (NodeRef) CACHE_ENTRY_NODE_REF.get(cacheValue);
            cacheUser = (RepositoryAuthenticatedUser) CACHE_ENTRY_USER_DETAILS.get(cacheValue);
            cacheExpiryDate = (Date) CACHE_ENTRY_CREDENTIALS_EXPIRY_DATE.get(cacheValue);

            Assert.assertNotSame(controlNode, cacheNode);
            Assert.assertEquals(controlNode, cacheNode);
            Assert.assertNotSame(controlUser, cacheUser);
            // no equals in the class hierarchy of the user
            Assert.assertEquals(controlUser.getUsername(), cacheUser.getUsername());
            Assert.assertEquals(controlUser.getPassword(), cacheUser.getPassword());
            Assert.assertEquals(controlUser.isEnabled(), cacheUser.isEnabled());
            Assert.assertEquals(controlUser.isAccountNonExpired(), cacheUser.isAccountNonExpired());
            Assert.assertEquals(controlUser.isCredentialsNonExpired(), cacheUser.isCredentialsNonExpired());
            Assert.assertEquals(controlUser.isAccountNonLocked(), cacheUser.isAccountNonLocked());
            Assert.assertEquals(controlUser.getHashIndicator(), cacheUser.getHashIndicator());
            Assert.assertEquals(controlUser.getSalt(), cacheUser.getSalt());

            Assert.assertNotSame(controlExpiryDate, cacheExpiryDate);
            Assert.assertEquals(controlExpiryDate, cacheExpiryDate);

            controlNode = new NodeRef(StoreRef.STORE_REF_WORKSPACE_SPACESSTORE, UUID.randomUUID().toString());
            controlUser = new RepositoryAuthenticatedUser("maxmuster", "differentHash", false, false, false, false,
                    new GrantedAuthority[] { new GrantedAuthorityImpl("role1"), new GrantedAuthorityImpl("role2"),
                            new GrantedAuthorityImpl("role3"), new GrantedAuthorityImpl("role4"), new GrantedAuthorityImpl("role5") },
                    Arrays.asList("123", "098", "019283", "564738"), UUID.randomUUID());
            controlExpiryDate = null;

            controlValue = CACHE_ENTRY_CTOR.newInstance(controlNode, controlUser, controlExpiryDate);
            cache.put("maxmuster", controlValue);
            cacheValue = cache.get("maxmuster");

            cacheNode = (NodeRef) CACHE_ENTRY_NODE_REF.get(cacheValue);
            cacheUser = (RepositoryAuthenticatedUser) CACHE_ENTRY_USER_DETAILS.get(cacheValue);
            cacheExpiryDate = (Date) CACHE_ENTRY_CREDENTIALS_EXPIRY_DATE.get(cacheValue);

            Assert.assertNotSame(controlNode, cacheNode);
            Assert.assertEquals(controlNode, cacheNode);
            Assert.assertNotSame(controlUser, cacheUser);
            // no equals in the class hierarchy of the user
            Assert.assertEquals(controlUser.getUsername(), cacheUser.getUsername());
            Assert.assertEquals(controlUser.getPassword(), cacheUser.getPassword());
            Assert.assertEquals(controlUser.isEnabled(), cacheUser.isEnabled());
            Assert.assertEquals(controlUser.isAccountNonExpired(), cacheUser.isAccountNonExpired());
            Assert.assertEquals(controlUser.isCredentialsNonExpired(), cacheUser.isCredentialsNonExpired());
            Assert.assertEquals(controlUser.isAccountNonLocked(), cacheUser.isAccountNonLocked());
            Assert.assertEquals(controlUser.getHashIndicator(), cacheUser.getHashIndicator());
            Assert.assertEquals(controlUser.getSalt(), cacheUser.getSalt());

            Assert.assertEquals(controlExpiryDate, cacheExpiryDate);
        }
    }

    protected void efficiencyImpl(final Ignite referenceGrid, final Ignite grid, final IgniteCache<String, Object> referenceCache,
            final IgniteCache<String, Object> cache, final String serialisationType, final String referenceSerialisationType,
            final double marginFraction) throws InstantiationException, InvocationTargetException, IllegalAccessException
    {
        LOGGER.info(
                "Running AuthenticationCacheEntry serialisation benchmark of 100k instances, comparing {} vs. {} serialisation, expecting relative improvement margin / difference fraction of {}",
                referenceSerialisationType, serialisationType, marginFraction);

        final SecureRandom rnJesus = new SecureRandom();
        final PasswordEncoder encoder = new MD4PasswordEncoderImpl();

        for (int idx = 0; idx < 5000; idx++)
        {
            final String salt = GUID.generate();
            final String password = encoder.encodePassword(UUID.randomUUID().toString(), salt);
            final NodeRef node = new NodeRef(StoreRef.STORE_REF_WORKSPACE_SPACESSTORE, UUID.randomUUID().toString());
            final RepositoryAuthenticatedUser user = new RepositoryAuthenticatedUser(UUID.randomUUID().toString(), password, true, true,
                    true, true,
                    // default Alfresco only ever uses a single granted authority
                    new GrantedAuthority[] { new GrantedAuthorityImpl("ROLE_AUTHENTICATED") }, CompositePasswordEncoder.MD4, salt);
            final Date credentialsExpiryDate = Date.from(Instant.now().plus(rnJesus.nextInt(365), ChronoUnit.DAYS));

            final Object value = CACHE_ENTRY_CTOR.newInstance(node, user, credentialsExpiryDate);

            cache.put(user.getUsername(), value);
            referenceCache.put(user.getUsername(), value);
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
