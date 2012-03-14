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

import java.io.File;

import org.neo4j.kernel.impl.nioneo.store.NeoStore;

public class RepairMissingId
{
    public static void main( String[] args ) throws Exception
    {
        boolean deleteDamaged = false;
        if ( args.length < 1 )
        {
            System.err.println( "You must supply a path to the database" );
            System.exit( 1 );
        }
        if ( args.length > 1 )
        {
            deleteDamaged = "repair".equalsIgnoreCase( args[1] );
        }
        File path = new File( args[0] );
        if ( !path.isDirectory() )
        {
            System.err.println( "You must supply a path as a first argument" );
            System.exit( 1 );
        }
        if ( !new File( path, NeoStore.DEFAULT_NAME ).exists() )
        {
            System.err.println( "You must supply a valid graph db path as a first argument" );
            System.exit( 1 );
        }
        System.out.println( "all is well, starting scan in directory " + path.getAbsolutePath() );
        if ( deleteDamaged )
        {
            System.out.println( "repair option was set: " + args[1]
                                + ", any documents without the id field will be deleted" );
        }
        IndexPaths indexPath = IndexPaths.fromRoot( path );
        for ( File index : indexPath.nodeIndexes() )
        {
            IndexRepair repair = new IndexRepair( index );
            repair.setDeleteDamaged( deleteDamaged );
            repair.scan();
        }
        for ( File index : indexPath.relationshipIndexes() )
        {
            IndexRepair repair = new IndexRepair( index );
            repair.setDeleteDamaged( deleteDamaged );
            repair.scan();
        }
    }
}
