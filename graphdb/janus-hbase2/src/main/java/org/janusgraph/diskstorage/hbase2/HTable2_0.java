// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.hbase2;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class HTable2_0
        implements TableMask
{
    Logger LOG = LoggerFactory.getLogger(HTable2_0.class);
    private final Table table;

    public HTable2_0(Table table)
    {
        this.table = table;
    }

    @Override
    public ResultScanner getScanner(Scan filter)
            throws IOException
    {
        if (LOG.isDebugEnabled()) {
            LOG.warn("Calling GetScanner");
        }
        return table.getScanner(filter);
    }

    @Override
    public Result[] get(List<Get> gets)
            throws IOException
    {

        Result[] rs = table.get(gets);

        if (LOG.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder("Calling get\tkey: ");
            append(builder, gets.get(0).getRow());
            builder.append("\tcolumn: ");
            for (byte[] bs : gets.get(0).familySet()) {
                builder.append(new String(bs));
                builder.append(",");
            }
            builder.append("\tfilter: ");
            append(builder, gets.get(0).getFilter().toByteArray());
//            builder.append("\n\tresults: ");
//            for (Cell cell : rs[0].rawCells()) {
//                builder.append("\n\t\tcolumn: ");
//                builder.append(new String(cell.getFamily()));
//                builder.append("\tqualifier: ");
//                append(builder, cell.getQualifier());
//                builder.append("\n\t\tvalue: ");
//                append(builder, cell.getValue());
//            }
            LOG.warn(builder.toString());
        }
        return rs;
    }

    private void append(StringBuilder builder, byte[] bytes)
    {
        for (byte b : bytes) {
            builder.append(b);
        }
    }

    @Override
    public void batch(List<Row> writes, Object[] results)
            throws IOException, InterruptedException
    {
        if (LOG.isDebugEnabled()) {
            LOG.warn("Calling batch, number: {},{}", writes.size(), results.length);
        }
        table.batch(writes, results);
        /* table.flushCommits(); not needed anymore */
    }

    @Override
    public void close()
            throws IOException
    {
        table.close();
    }
}
