package com.clickhouse.kafka.connect.sink.hashing;

import com.clickhouse.kafka.connect.sink.processing.Processing;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.util.Set;

public class RecordHash {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordHash.class);
    private SinkRecord record = null;
    private int n_buckets = 0;
    private String HashFunctionName;
    private String hashFunctionNameDefault = "default";

    public RecordHash(SinkRecord record, int n_buckets){
        this.record = record;
        this.n_buckets = n_buckets;
        this.HashFunctionName = hashFunctionNameDefault;
    }

    public boolean setFunctionName(String name) {
        if (!name.equals(hashFunctionNameDefault)) {
            try {
                MessageDigest.getInstance(name);
            } catch (NoSuchAlgorithmException error) {
                LOGGER.info(String.format("failed to use hash function %s ", name) + error);
                return false;
            }
        }
        this.HashFunctionName = name;
        return true;
    }

    public Set<String> availableHashAlgorithms() {
        Set<String> messageDigest = Security.getAlgorithms("MessageDigest");
        return messageDigest;
    }

    public int getHash() throws NoSuchAlgorithmException {
        if (this.HashFunctionName.equals(hashFunctionNameDefault)) {
            return this.record.hashCode();
        }

        MessageDigest md = MessageDigest.getInstance(this.HashFunctionName);
        md.update(this.record.toString().getBytes());
        return Bytes.wrap(md.digest()).hashCode();
    }

    public int getBucketIndex() {
        if (valid()) {
            try {
                return Math.abs(this.getHash()) % this.n_buckets;
            } catch (NoSuchAlgorithmException error) {
                LOGGER.error(String.format("failed to use hash %s into one of %s buckets", this.record, this.n_buckets) + error);
            }
        }
        return -1;
    }

    private boolean valid() {
        if (this.n_buckets<1){
            LOGGER.error("number of buckets needs to be a positive integer not: "+this.n_buckets);
            return false;
        }
        return true;
    }
}
