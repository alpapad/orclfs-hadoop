package org.apache.hadoop.fs.contract.orclfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractGetFileStatusTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class TestOrclFSContractGetFileStatusTest extends AbstractContractGetFileStatusTest {
    
    @Override
    protected AbstractFSContract createContract(Configuration conf) {
        return new OrclFSContract(conf);
    }
    
}
