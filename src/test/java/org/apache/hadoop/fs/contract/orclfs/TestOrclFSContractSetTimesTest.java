/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract.orclfs;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractSetTimesTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.Test;

public class TestOrclFSContractSetTimesTest extends AbstractContractSetTimesTest {
    @Override
    protected AbstractFSContract createContract(Configuration conf) {
        return new OrclFSContract(conf);
    }
    
    @Test
    public void testSetTimesExistentFile() throws Throwable {
        describe("create & set times a 0 byte file");
        Path path = path("zero.txt");
        try {

            getFileSystem().setWorkingDirectory(new Path("/"));
            touch(getFileSystem(), path);
            long time = new Date().getTime();
            getFileSystem().setTimes(path, time, time);
            FileStatus stat = getFileSystem().getFileStatus(path);
            assertEquals(time,  stat.getAccessTime());
            assertEquals(time,  stat.getModificationTime());
        } finally {
            getFileSystem().delete(path, true);
        }
    }
}
