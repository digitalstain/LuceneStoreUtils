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
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.logging.Logger;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

public class IndexRepair
{
    private static final Logger log = Logger.getLogger( IndexRepair.class.getName() );
    private static final String IdField = "_id_";

    private final FSDirectory dir;
    private final Collection<Document> damagedDocs;
    private final IndexReader reader;

    private int deletedDocs;
    private int scannedDocs;
    private boolean deleteDamaged;

    public IndexRepair(File indexDir) throws IOException
    {
        dir = FSDirectory.open( indexDir );
        damagedDocs = new HashSet<Document>();
        reader = IndexReader.open( dir, false );
        deleteDamaged = false;
    }

    public void setDeleteDamaged( boolean deleteDamaged )
    {
        this.deleteDamaged = deleteDamaged;
    }

    public boolean getDeleteDamaged()
    {
        return deleteDamaged;
    }

    public void scan() throws IOException
    {
        log.info( "Opened it, it contains " + reader.maxDoc()
                  + " documents. Iterating over them" );
        for ( int i = 0; i < reader.maxDoc(); i++ )
        {
            scannedDocs++;
            if ( reader.isDeleted( i ) )
            {
                deletedDocs++;
                continue;
            }
            Document current = reader.document( i );
            if ( isDamaged( current ) )
            {
                handleDamaged( reader, i, current );
            }

        }
        reader.commit( null );
        reader.close();
        log.info( "Index " + dir.getDirectory().getAbsolutePath() + " done. Scanned " + scannedDocs
                  + " documents, there were" + deletedDocs + " deleted ones which were ignored" );
    }

    public int getDamagedCount()
    {
        return damagedDocs.size();
    }

    public int getTotalCount()
    {
        return scannedDocs;
    }

    private boolean isDamaged( Document doc )
    {
        return doc.getFieldable( IdField ) == null;
    }

    private void handleDamaged( IndexReader reader, int docId, Document doc ) throws IOException
    {
        damagedDocs.add( doc );
        if ( deleteDamaged )
        {
            reader.deleteDocument( docId );
        }
    }
}
