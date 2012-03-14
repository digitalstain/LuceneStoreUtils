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

public class IndexPaths
{
    private static final String IndexDirectoryName = "index";
    private static final String LuceneIndexDirectoryName = "lucene";
    private static final String NodeIndexesDirectoryName = "node";

    private final File root;

    private IndexPaths( File root )
    {
        this.root = new File( new File( root, IndexDirectoryName ), LuceneIndexDirectoryName );
    }

    public File forNode( String nodeIndexName )
    {
        return new File( new File( root, NodeIndexesDirectoryName ), nodeIndexName );
    }

    public static IndexPaths fromRoot( File root )
    {
        return new IndexPaths( root );
    }
}
