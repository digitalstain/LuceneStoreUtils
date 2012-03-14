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
package org.neo4j.index.lucene.repair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.index.lucene.repair.util.GraphDatabaseHandler;
import org.neo4j.test.TargetDirectory;

public class TestCommandLineTool
{
    @Test
    public void comprehensiveRepairTest() throws Exception
    {
        // build phase
        File storeDir = TargetDirectory.forTest( getClass() ).directory( "comprehensiveRepairTest", true );
        GraphDatabaseHandler db = new GraphDatabaseHandler( storeDir );

        String nodeIndex1 = "node1";
        db.createNodeIndex( nodeIndex1 );
        for ( int i = 0; i < 100; i++ )
        {
            db.createAndIndexNode( nodeIndex1, "key" + i, "value" + i, false );
        }
        db.shutdown();

        // damage phase

        long thatWillBeDamaged = 50;
        IndexPaths paths = IndexPaths.fromRoot( storeDir );

        IndexHandler indexHandler = new IndexHandler( paths.forNode( nodeIndex1 ) );
        indexHandler.deleteFieldFromNodeDocument( thatWillBeDamaged + 1, "_id_" );

        // repair phase
        RepairMissingId.main( new String[] { storeDir.getAbsolutePath(), "true" } );

        // verify phase
        db.start();
        for ( int i = 0; i < 100; i++ )
        {
            Node node = db.getUniqueFromNodeIndex( nodeIndex1, "key" + i, "value" + i );
            if ( i == thatWillBeDamaged )
            {
                assertNull( "index value should not be here", node );
            }
            else
            {
                assertEquals( "missing index value", i + 1, node.getId() );
            }
        }
    }

    @Test
    public void doesNotTouchIndexesUnlessToldToDoSo() throws Exception
    {
        // build phase
        File storeDir = TargetDirectory.forTest( getClass() ).directory( "doesNotTouchIndexesUnlessToldToDoSo", true );
        GraphDatabaseHandler db = new GraphDatabaseHandler( storeDir );

        String nodeIndex1 = "node1";
        db.createNodeIndex( nodeIndex1 );
        for ( int i = 0; i < 100; i++ )
        {
            db.createAndIndexNode( nodeIndex1, "key" + i, "value" + i, false );
        }
        db.shutdown();

        // damage phase

        long thatWillBeDamaged = 50;
        IndexPaths paths = IndexPaths.fromRoot( storeDir );

        IndexHandler indexHandler = new IndexHandler( paths.forNode( nodeIndex1 ) );
        indexHandler.deleteFieldFromNodeDocument( thatWillBeDamaged + 1, "_id_" );

        // repair phase
        RepairMissingId.main( new String[] { storeDir.getAbsolutePath(), "false" } );

        // verify phase
        db.start();
        for ( int i = 0; i < 100; i++ )
        {
            if ( i == thatWillBeDamaged )
            {
                try
                {
                    db.getUniqueFromNodeIndex( nodeIndex1, "key" + i, "value" + i );
                    fail( "The damaged document should still be there" );
                }
                catch ( NumberFormatException e )
                {
                    // good, stil damaged
                }
            }
            else
            {
                assertEquals( "missing index value", i + 1,
                        db.getUniqueFromNodeIndex( nodeIndex1, "key" + i, "value" + i ).getId() );
            }
        }
    }
}
