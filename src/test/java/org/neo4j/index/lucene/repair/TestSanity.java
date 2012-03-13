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

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.test.TargetDirectory;

public class TestSanity
{
    private File dbDir;
    private GraphDatabaseService db;
    private FSDirectory dir;

    @Before
    public void setupMissingId() throws Exception
    {
        dbDir = TargetDirectory.forTest( getClass() ).directory( "shouldWork", true );
        db = new EmbeddedGraphDatabase( dbDir.getAbsolutePath() );
        Transaction tx = db.beginTx();
        Index<Node> index = db.index().forNodes( "testing" );
        index.add( db.createNode(), "key1", "value1" );
        index.add( db.createNode(), "key2", "value2" );
        tx.success();
        tx.finish();
        db.shutdown();

        dir = FSDirectory.open( new File( dbDir, "index/lucene/node/testing" ) );
        IndexReader reader = IndexReader.open( dir, false );

        System.out.println( "Opened it, it contains " + reader.maxDoc() + " documents. Iterating over them" );
        Document newDoc = null;

        for ( int i = 0; i < reader.maxDoc(); i++ )
        {
            Document current = reader.document( i );
            Field id = current.getField( "_id_" );
            if ( id.stringValue().equals( "2" ) )
            {
                newDoc = new Document();
                for ( Fieldable f : current.getFields() )
                {
                    if ( !f.name().equals( "_id_" ) )
                    {
                        newDoc.add( f );
                    }
                }
                reader.deleteDocument( i );
            }
        }
        reader.commit( null );
        reader.close();

        IndexWriter writer = new IndexWriter( dir, new IndexWriterConfig( Version.LUCENE_35, new WhitespaceAnalyzer(
                Version.LUCENE_35 ) ) );
        writer.addDocument( newDoc );
        writer.commit();
        writer.close();

        db = new EmbeddedGraphDatabase( dbDir.getAbsolutePath() );
    }

    @After
    public void shutDownEverything()
    {
        db.shutdown();
        dir.close();
    }

    private void goOverBoth()
    {
        Index<Node> index = db.index().forNodes( "testing" );
        IndexHits<Node> hits = index.get( "key2", "value2" );
        try
        {
            for ( Node n : hits )
            {
                System.out.println( n.getId() );
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test( expected = java.lang.NumberFormatException.class )
    public void shouldFailOnNoAction() throws Exception
    {
        goOverBoth();
    }
}
