/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.hive.bridge;

import com.google.common.base.Preconditions;
import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import org.apache.hadoop.hive.conf.HiveConf;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HiveMetaStoreConnector
{
    private static BoneCP pool;
    private static String sql = "select p.part_id,p.sd_id,p.create_time,p.last_access_time,p.part_name,s.input_format,s.location,s.is_compressed,s.is_storedassubdirectories," +
            "s.num_buckets,s.output_format,sd.name as sd_name,sd.slib from PARTITIONS p join SDS s on p.sd_id=s.sd_id join TBLS t on p.tbl_id =t.tbl_id join DBS d on t.db_id=d.db_id " +
            "left join SERDES sd on s.serde_id=sd.serde_id where d.name=? and t.tbl_name=?;";

    public HiveMetaStoreConnector(HiveConf conf)
            throws ClassNotFoundException, SQLException
    {
        Class.forName(Preconditions.checkNotNull(conf.get("javax.jdo.option.ConnectionDriverName")).split("\\?")[0]);
        BoneCPConfig config = new BoneCPConfig();
        config.setJdbcUrl(Preconditions.checkNotNull(conf.get("javax.jdo.option.ConnectionURL")));
        config.setUsername(Preconditions.checkNotNull(conf.get("javax.jdo.option.ConnectionUserName")));
        config.setPassword(Preconditions.checkNotNull(conf.get("javax.jdo.option.ConnectionPassword")));
        config.setMinConnectionsPerPartition(0);
        config.setMaxConnectionsPerPartition(1);
        pool = new BoneCP(config);
    }

    public List<PartitionInfo> getPartitionInfos(String db, String table)
            throws SQLException
    {
        try (Connection connection = pool.getConnection()) {
            List<PartitionInfo> results = new ArrayList<>();
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1, db);
            stmt.setString(2, table);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                results.add(new PartitionInfo(db, table, rs));
            }
            return results;
        }
    }

    public static class PartitionInfo
    {
        public final String dbName;
        public final String tableName;
        public final Long partId;
        public final Long sdId;
        public final Date createTime;
        public final Date lastAccessTime;
        public final String partName;
        public final String inputFormat;
        public final String location;
        public final boolean isCompressed;
        public final boolean isStoredAsSubdirectories;
        public final Integer numBuckets;
        public final String outputFormat;
        public final String sdName;
        public final String sdLib;

        public PartitionInfo(String dbName, String tableName, ResultSet rs)
                throws SQLException
        {
            this.dbName = dbName;
            this.tableName = tableName;
            partId = rs.getLong("part_id");
            sdId = rs.getLong("sd_id");
            createTime = new Date(rs.getInt("create_time") * 1000L);
            lastAccessTime = new Date(rs.getInt("last_access_time") * 1000L);
            partName = rs.getString("part_name");
            inputFormat = rs.getString("input_format");
            location = rs.getString("location");
            isCompressed = rs.getBoolean("is_compressed");
            isStoredAsSubdirectories = rs.getBoolean("is_storedassubdirectories");
            numBuckets = rs.getInt("num_buckets");
            outputFormat = rs.getString("output_format");
            sdName = rs.getString("sd_name");
            sdLib = rs.getString("slib");
        }
    }
}

