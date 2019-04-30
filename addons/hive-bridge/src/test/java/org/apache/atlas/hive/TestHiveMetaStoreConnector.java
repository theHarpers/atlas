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
package org.apache.atlas.hive;

import org.apache.atlas.hive.bridge.HiveMetaStoreConnector;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

import java.sql.SQLException;

public class TestHiveMetaStoreConnector
{
    @Test
    public void test()
            throws SQLException, ClassNotFoundException
    {
        HiveMetaStoreConnector connector = new HiveMetaStoreConnector(new HiveConf());
        for (HiveMetaStoreConnector.PartitionInfo info : connector.getPartitionInfos("aggregate", "inventory")) {
            System.out.println(String.format("d%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s", info.dbName, info.tableName, info.sdLib, info.sdName,
                    info.partId, info.createTime, info.inputFormat, info.isCompressed, info.isStoredAsSubdirectories, info.lastAccessTime,
                    info.location, info.numBuckets, info.outputFormat, info.partName));
        }
    }
}
