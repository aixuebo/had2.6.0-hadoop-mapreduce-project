/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * A RecordReader that reads records from a MySQL table.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MySQLDBRecordReader<T extends DBWritable> extends DBRecordReader<T> {

  /**
   * @param split The InputSplit to read data for 数据库被序列化后的文件InputSplit
   * @param inputClass value对应的序列化对象
   * @param conf
   * @param conn 数据库连接Connection
   * @param dbConfig
   * @param cond where条件集合
   * @param fields select字段集合
   * @param table 数据库表
   */
  public MySQLDBRecordReader(DBInputFormat.DBInputSplit split, 
      Class<T> inputClass, Configuration conf, Connection conn, DBConfiguration dbConfig,
      String cond, String [] fields, String table) throws SQLException {
    super(split, inputClass, conf, conn, dbConfig, cond, fields, table);
  }

  // Execute statements for mysql in unbuffered mode.执行sql,返回ResultSet
  protected ResultSet executeQuery(String query) throws SQLException {
    statement = getConnection().prepareStatement(query,
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    statement.setFetchSize(Integer.MIN_VALUE); // MySQL: read row-at-a-time.
    return statement.executeQuery();
  }
}
