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
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class IndexHandler
{
    private final Directory dir;

    public IndexHandler( File index ) throws Exception
    {
        dir = FSDirectory.open( index );
    }

    //
    // public abstract boolean deleteDocument( int docId );
    //
    // public abstract void deleteFieldFromDocument( Fieldable field, Document
    // doc );

    public void deleteFieldFromNodeDocument( long nodeId, String fieldName ) throws Exception
    {
        IndexReader reader = IndexReader.open( dir, false );
        Document newDoc = null;
        IndexSearcher searcher = new IndexSearcher( reader );
        TopDocs searchResult = searcher.search( new TermQuery( new Term( "_id_", Long.toString( nodeId ) ) ), 2 );

        if ( searchResult.totalHits > 1 )
        {
            throw new IllegalStateException( "There should be only one hit for node id " + nodeId
                                             + ", i got at least 2" );
        }

        int docId = searchResult.scoreDocs[0].doc;
        Document original = reader.document( docId );

        newDoc = new Document();
        for ( Fieldable f : original.getFields() )
        {
            if ( !f.name().equals( fieldName ) )
            {
                newDoc.add( f );
            }
        }
        reader.deleteDocument( docId );
        reader.commit( null );
        reader.close();

        IndexWriter writer = new IndexWriter( dir, new IndexWriterConfig( Version.LUCENE_35, new WhitespaceAnalyzer(
                Version.LUCENE_35 ) ) );
        writer.addDocument( newDoc );
        writer.commit();
        writer.close();
    }

    // public abstract void deleteFieldFromRelationshipDocument( long relId,
    // String fieldName );
    //
    // abstract void start();

    // public abstract void close();
}
