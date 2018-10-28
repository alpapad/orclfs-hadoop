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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.test.connector.HcfsTestConnector;

/**
 * The contract of the Local filesystem. This changes its feature set from
 * platform for platform -the default set is updated during initialization.
 *
 * This contract contains some override points, to permit the raw local
 * filesystem and other filesystems to subclass it.
 */
public class OrclFSContract extends AbstractFSContract {
    
    public static final String CONTRACT_XML = "contract/orclfs.xml";
    
    public static final String SYSPROP_TEST_BUILD_DATA = "test.build.data";
    public static final String DEFAULT_TEST_BUILD_DATA_DIR = "test/build/data";
    private FileSystem fs;
    
    public OrclFSContract(Configuration conf) {
        super(conf);
        // insert the base features
        addConfResource(getContractXml());
    }
    
    /**
     * Return the contract file for this filesystem
     * 
     * @return the XML
     */
    protected String getContractXml() {
        return CONTRACT_XML;
    }
    
    @Override
    public void init() throws IOException {
        super.init();
        fs = new HcfsTestConnector().create();
    }
    
    @Override
    public String getScheme() {
        return "orcl";
    }
    
    @Override
    public Path getTestPath() {
        Path path = fs.makeQualified(new Path(getTestDataDir()));
        return path;
    }
    
    /**
     * Get the test data directory
     * 
     * @return the directory for test data
     */
    protected String getTestDataDir() {
        return System.getProperty(SYSPROP_TEST_BUILD_DATA, DEFAULT_TEST_BUILD_DATA_DIR);
    }
    
    @Override
    public FileSystem getTestFileSystem() throws IOException {
        return fs;
    }
}
