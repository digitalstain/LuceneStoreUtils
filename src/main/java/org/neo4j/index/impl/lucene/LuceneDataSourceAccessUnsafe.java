/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import org.apache.lucene.index.IndexWriter;
import org.neo4j.kernel.AbstractGraphDatabase;

public class LuceneDataSourceAccessUnsafe
{
    private final LuceneDataSource ds;

    public LuceneDataSourceAccessUnsafe( AbstractGraphDatabase db )
    {
        ds = (LuceneDataSource) db.getConfig().getTxModule().getXaDataSourceManager().getXaDataSource(
                LuceneDataSource.DEFAULT_NAME );
    }

    public IndexWriter getWriterFor( IndexTypeEnum primitiveType, String indexName )
    {
        byte entityTypeByte;
        EntityType entityType;
        switch ( primitiveType )
        {
        case Node:
            entityTypeByte = LuceneCommand.NODE;
            entityType = ds.nodeEntityType;
            break;
        case Relationship:
            entityTypeByte = LuceneCommand.RELATIONSHIP;
            entityType = ds.relationshipEntityType;
            break;
        default:
            throw new IllegalArgumentException( "How am i supposed to deal with " + primitiveType );
        }

        return ds.getIndexWriter( new IndexIdentifier( entityTypeByte, entityType, indexName ) );
    }
}
