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

import java.io.File;

import org.junit.Test;
import org.neo4j.index.lucene.repair.util.GraphDatabaseHandler;
import org.neo4j.test.TargetDirectory;

public class TestCorrectness
{
    @Test
    public void testLeavesValidIndexesIntact() throws Exception
    {
        File storeDir = TargetDirectory.forTest( getClass() ).directory( "testLeavesValidIndexesIntact", true );
        GraphDatabaseHandler db = new GraphDatabaseHandler( storeDir );

        String nodeIndex1 = "node1";
        db.createNodeIndex( nodeIndex1 );
        long node1 = db.createAndIndexNode( nodeIndex1, "key1", "value1", false );

        String nodeIndex2 = "node2";
        db.createNodeIndex( nodeIndex2 );
        long node2 = db.createAndIndexNode( nodeIndex2, "key2", "value2", false );

        String relationshipIndex = "relationship";
        db.createRelationshipIndex( relationshipIndex );
        long relationship = db.createAndIndexRelationship( relationshipIndex, node1, node2, "relType", "key", "value",
                false );

        db.shutdown();
        RepairMissingId.main( new String[] { storeDir.getAbsolutePath() } );
        db.start();

        assertEquals( "missing index value", node1, db.getUniqueFromNodeIndex( nodeIndex1, "key1", "value1" ).getId() );
        assertEquals( "missing index value", node2, db.getUniqueFromNodeIndex( nodeIndex2, "key2", "value2" ).getId() );
        assertEquals( "missing index value", relationship,
                db.getUniqueFromRelationshipIndex( relationshipIndex, "key", "value" ).getId() );

        db.shutdown();
    }

    @Test
    public void testDetectsDamagedFields() throws Exception
    {
        File storeDir = TargetDirectory.forTest( getClass() ).directory( "testDetectsDamagedFields", true );
        GraphDatabaseHandler db = new GraphDatabaseHandler( storeDir );

        String nodeIndex1 = "node1";
        db.createNodeIndex( nodeIndex1 );
        long node1 = db.createAndIndexNode( nodeIndex1, "key1", "value1", false );
        db.createAndIndexNode( nodeIndex1, "key2", "value2", false );
        db.shutdown();

        IndexPaths paths = IndexPaths.fromRoot( storeDir );

        IndexHandler indexHandler = new IndexHandler( paths.forNode( nodeIndex1 ) );
        indexHandler.deleteFieldFromNodeDocument( node1, "_id_" );

        IndexRepair repair = new IndexRepair(paths.forNode( nodeIndex1 ));
        repair.scan();
        assertEquals( "did not detect damaged docs", 1, repair.getDamagedCount() );
        assertEquals( "did not detect docs without problems", 2, repair.getTotalCount() );
    }

    @Test
    public void testRemovesDamagedFields() throws Exception
    {
        File storeDir = TargetDirectory.forTest( getClass() ).directory( "testDetectsDamagedFields", true );
        GraphDatabaseHandler db = new GraphDatabaseHandler( storeDir );

        String nodeIndex1 = "node1";
        db.createNodeIndex( nodeIndex1 );
        long node1 = db.createAndIndexNode( nodeIndex1, "key1", "value1", false );
        long node2 = db.createAndIndexNode( nodeIndex1, "key2", "value2", false );
        db.shutdown();

        IndexPaths paths = IndexPaths.fromRoot( storeDir );

        IndexHandler indexHandler = new IndexHandler( paths.forNode( nodeIndex1 ) );
        indexHandler.deleteFieldFromNodeDocument( node1, "_id_" );

        IndexRepair repair = new IndexRepair( paths.forNode( nodeIndex1 ) );
        repair.setDeleteDamaged( true );
        repair.scan();

        db.start();
        assertNull( "index value should not be here", db.getUniqueFromNodeIndex( nodeIndex1, "key1", "value1" ) );
        assertEquals( "missing index value", node2, db.getUniqueFromNodeIndex( nodeIndex1, "key2", "value2" ).getId() );
        db.shutdown();
    }
}
