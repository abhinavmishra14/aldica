/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
package org.aldica.repo.ignite.binary.entity;

import org.aldica.repo.ignite.binary.base.AbstractStoreCustomBinarySerializer;
import org.alfresco.repo.domain.node.NodeEntity;
import org.alfresco.repo.domain.node.StoreEntity;
import org.alfresco.util.Pair;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;

/**
 * Instances of this class handle (de-)serialisations of {@link StoreEntity} instances in order to optimise their serial form. This
 * implementation primarily aims to optimise handling of well-known {@link StoreEntity#getProtocol() protocols} and
 * {@link StoreEntity#getIdentifier() identifiers} as part of the store identity.
 *
 * @author Axel Faust
 */
public class StoreEntityBinarySerializer extends AbstractStoreCustomBinarySerializer
{

    private static final String ID = "id";

    private static final String VERSION = "version";

    private static final String ROOT_NODE = "rootNode";

    /**
     *
     * {@inheritDoc}
     */
    @Override
    public void writeBinary(final Object obj, final BinaryWriter writer) throws BinaryObjectException
    {
        final Class<? extends Object> cls = obj.getClass();
        if (!cls.equals(StoreEntity.class))
        {
            throw new BinaryObjectException(cls + " is not supported by this serializer");
        }

        final StoreEntity store = (StoreEntity) obj;

        if (this.useRawSerialForm)
        {
            final BinaryRawWriter rawWriter = writer.rawWriter();

            // version for optimistic locking is effectively a DB ID as well
            this.writeDbId(store.getId(), rawWriter);
            this.writeDbId(store.getVersion(), rawWriter);
            this.writeStore(store.getProtocol(), store.getIdentifier(), rawWriter);
            rawWriter.writeObject(store.getRootNode());
        }
        else
        {
            writer.writeLong(ID, store.getId());
            writer.writeLong(VERSION, store.getVersion());
            this.writeStore(store.getProtocol(), store.getIdentifier(), writer);
            writer.writeObject(ROOT_NODE, store.getRootNode());
        }
    }

    /**
     *
     * {@inheritDoc}
     */
    @Override
    public void readBinary(final Object obj, final BinaryReader reader) throws BinaryObjectException
    {
        final Class<? extends Object> cls = obj.getClass();
        if (!cls.equals(StoreEntity.class))
        {
            throw new BinaryObjectException(cls + " is not supported by this serializer");
        }

        final StoreEntity store = (StoreEntity) obj;

        Long id;
        Long version;
        Pair<String, String> protocolAndIdentifier;
        NodeEntity rootNode;

        if (this.useRawSerialForm)
        {
            final BinaryRawReader rawReader = reader.rawReader();

            id = this.readDbId(rawReader);
            version = this.readDbId(rawReader);
            protocolAndIdentifier = this.readStore(rawReader);
            rootNode = rawReader.readObject();
        }
        else
        {
            id = reader.readLong(ID);
            version = reader.readLong(VERSION);
            protocolAndIdentifier = this.readStore(reader);
            rootNode = reader.readObject(ROOT_NODE);
        }

        store.setId(id);
        store.setVersion(version);
        store.setProtocol(protocolAndIdentifier.getFirst());
        store.setIdentifier(protocolAndIdentifier.getSecond());
        store.setRootNode(rootNode);
    }

}
