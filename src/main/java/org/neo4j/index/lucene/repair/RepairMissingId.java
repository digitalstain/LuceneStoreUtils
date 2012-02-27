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
import java.util.logging.Logger;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;

public class RepairMissingId
{
    private static final Logger log = Logger.getLogger( RepairMissingId.class.getName() );
    private static final String IdField = "_id_";

    public static void main( String[] args ) throws Exception
    {
        if ( args.length < 1 )
        {
            System.err.println( "You must supply a path to the database" );
            System.exit( 1 );
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
        File nodes = new File( path, "index/lucene/node" );
        if ( nodes.isDirectory() && nodes.exists() )
        {
            for ( File index : nodes.listFiles() )
            {
                new RepairMissingId().doIndex( index );
            }
        }
        File relationships = new File( path, "index/lucene/relationship" );
        if ( relationships.isDirectory() && relationships.exists() )
        {
            for ( File index : relationships.listFiles() )
            {
                new RepairMissingId().doIndex( index );
            }
        }
    }

    private void doIndex( File pathToIndex ) throws Exception
    {
        log.info( "Checking " + pathToIndex );
        FSDirectory dir = FSDirectory.open( pathToIndex );
        IndexReader reader = IndexReader.open( dir, false );

        System.out.println( "Opened it, it contains " + reader.maxDoc()
                            + " documents. Iterating over them" );
        log.info( "Opened it, it contains " + reader.maxDoc()
                  + " documents. Iterating over them" );
        int deleted = 0;
        for ( int i = 0; i < reader.maxDoc(); i++ )
        {
            if ( reader.isDeleted( i ) )
            {
                deleted++;
                continue;
            }
            Document current = reader.document( i );
            Field id = current.getField( IdField );
            if ( id == null )
            {
                handleDamaged( current );
                reader.deleteDocument( i );
            }
        }
        reader.commit( null );
        reader.close();
        log.info( "Index " + pathToIndex + " done. Found " + deleted
                  + " deleted documents which were ignored" );
    }

    private void handleDamaged( Document doc )
    {
        log.warning( "Got a document without the _id_ field. These are the fields and values it contains" );
        for ( Fieldable f : doc.getFields() )
        {
            log.info( "\tfield: " + f.name() + ", value: " + f.stringValue() );
        }
    }
}
