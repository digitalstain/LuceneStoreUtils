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
package org.neo4j.index.lucene.repair.util;

import java.io.File;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class GraphDatabaseHandler
{
    private GraphDatabaseService db;
    private final File storeDir;

    public GraphDatabaseHandler( File storeDir )
    {
        this.storeDir = storeDir;
        start();
    }

    public void createNodeIndex( String indexName )
    {
        Transaction tx = db.beginTx();
        try
        {
            db.index().forNodes( indexName );
            tx.success();
        }
        catch ( Throwable t )
        {
            tx.failure();
            throw new RuntimeException( t );
        }
        finally
        {
            tx.finish();
        }
    }

    public void createRelationshipIndex( String indexName )
    {
        Transaction tx = db.beginTx();
        try
        {
            db.index().forRelationships( indexName );
            tx.success();
        }
        catch ( Throwable t )
        {
            tx.failure();
            throw new RuntimeException( t );
        }
        finally
        {
            tx.finish();
        }
    }

    public long createAndIndexNode( String nodeIndex, String key, Object value, boolean addAsProperty )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node n = db.createNode();
            db.index().forNodes( nodeIndex ).add( n, key, value );
            if ( addAsProperty )
            {
                n.setProperty( key, value );
            }
            tx.success();
            return n.getId();
        }
        catch ( Throwable t )
        {
            tx.failure();
            throw new RuntimeException( t );
        }
        finally
        {
            tx.finish();
        }
    }

    public long createAndIndexRelationship( String relationshipIndex, long nodeFrom, long nodeTo,
            String relType, String key, Object value, boolean addAsProperty )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node from = db.getNodeById( nodeFrom );
            Node to = db.getNodeById( nodeTo );
            Relationship result = from.createRelationshipTo( to, DynamicRelationshipType.withName( relType ) );
            db.index().forRelationships( relationshipIndex ).add( result, key, value );
            if ( addAsProperty )
            {
                result.setProperty( key, value );
            }
            tx.success();
            return result.getId();
        }
        catch ( Throwable t )
        {
            tx.failure();
            throw new RuntimeException( t );
        }
        finally
        {
            tx.finish();
        }
    }

    public Node getUniqueFromNodeIndex( String nodeIndex, String key, String value )
    {
        return db.index().forNodes( nodeIndex ).get( key, value ).getSingle();
    }

    public Relationship getUniqueFromRelationshipIndex( String relationshipIndex, String key, String value )
    {
        return db.index().forRelationships( relationshipIndex ).get( key, value ).getSingle();
    }

    public void shutdown()
    {
        db.shutdown();
    }

    public void start()
    {
        db = new EmbeddedGraphDatabase( storeDir.getAbsolutePath() );
    }

    public void restart()
    {
        shutdown();
        start();
    }

    public AbstractGraphDatabase getAsAGD()
    {
        return (AbstractGraphDatabase) db;
    }
}
