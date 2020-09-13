/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
package org.aldica.repo.ignite.binary.entity;

import org.aldica.repo.ignite.binary.base.AbstractCustomBinarySerializer;
import org.alfresco.repo.domain.node.TransactionEntity;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;

/**
 * Instances of this class handle (de-)serialisations of {@link TransactionEntity} instances in order to optimise their serial form.
 *
 * @author Axel Faust
 */
public class TransactionEntityBinarySerializer extends AbstractCustomBinarySerializer
{

    private static final String ID = "id";

    private static final String VERSION = "version";

    private static final String CHANGE_TXN_ID = "changeTxnId";

    private static final String COMMIT_TIME_MS = "commitTimeMs";

    /**
     *
     * {@inheritDoc}
     */
    @Override
    public void writeBinary(final Object obj, final BinaryWriter writer) throws BinaryObjectException
    {
        final Class<? extends Object> cls = obj.getClass();
        if (!cls.equals(TransactionEntity.class))
        {
            throw new BinaryObjectException(cls + " is not supported by this serializer");
        }

        final TransactionEntity transaction = (TransactionEntity) obj;

        if (this.useRawSerialForm)
        {
            final BinaryRawWriter rawWriter = writer.rawWriter();

            // version for optimistic locking is effectively a DB ID as well
            this.writeDbId(transaction.getId(), rawWriter);
            this.writeDbId(transaction.getVersion(), rawWriter);
            this.write(transaction.getChangeTxnId(), rawWriter);
            // highly unlikely (impossible without DB manipulation) to have a commit time ms before 1970
            // 64 bit timestamp can also live with some bits knocked off for potential serial optimisation if enabled
            this.write(transaction.getCommitTimeMs(), true, rawWriter);
        }
        else
        {
            writer.writeLong(ID, transaction.getId());
            writer.writeLong(VERSION, transaction.getVersion());
            writer.writeString(CHANGE_TXN_ID, transaction.getChangeTxnId());
            writer.writeLong(COMMIT_TIME_MS, transaction.getCommitTimeMs());
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
        if (!cls.equals(TransactionEntity.class))
        {
            throw new BinaryObjectException(cls + " is not supported by this serializer");
        }

        final TransactionEntity transaction = (TransactionEntity) obj;

        Long id;
        Long version;
        String changeTxnId;
        Long commitTimeMs;

        if (this.useRawSerialForm)
        {
            final BinaryRawReader rawReader = reader.rawReader();

            id = this.readDbId(rawReader);
            version = this.readDbId(rawReader);
            changeTxnId = this.readString(rawReader);
            commitTimeMs = this.readLong(true, rawReader);
        }
        else
        {
            id = reader.readLong(ID);
            version = reader.readLong(VERSION);
            changeTxnId = reader.readString(CHANGE_TXN_ID);
            commitTimeMs = reader.readLong(COMMIT_TIME_MS);
        }

        transaction.setId(id);
        transaction.setVersion(version);
        transaction.setChangeTxnId(changeTxnId);
        transaction.setCommitTimeMs(commitTimeMs);
    }

}
