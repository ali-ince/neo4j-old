/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.io.pagecache.tracing.cursor.context;

/**
 * Context that contains state of ongoing versioned data read or write.
 *
 * Read context performing data read with a version that is equal to last closed transaction.
 * Write context data modification with have version that is equal to transaction that is currently committing.
 *
 * As soon reader that associated with a context will observe data version that it should not see, context will be
 * marked as dirty.
 */
public interface VersionContext
{
    /**
     * Initialise read context with latest closed transaction id as it current version.
     */
    void initRead();

    /**
     * Initialise write context with committingTxId as modification version.
     * @param committingTxId currently committing transaction id
     */
    void initWrite( long committingTxId );

    /**
     * Context currently committing transaction id
     * @return committing transaction id
     */
    long committingTransactionId();

    /**
     * Last closed transaction id that read context was initialised with
     * @return last closed transaction id
     */
    long lastClosedTransactionId();

    /**
     * Mark current context as dirty
     */
    void markAsDirty();

    /**
     * Check whenever current context is dirty
     * @return true if context is dirty, false otherwise
     */
    boolean isDirty();

}
