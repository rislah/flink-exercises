package org.rislah;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.HashSet;
import java.util.Set;

public class PageViewAccumulator extends Tuple3<String, Integer, Set<String>> {
    public PageViewAccumulator() {
        super("", 0, new HashSet<>(2048));
    }

    @Override
    public String toString() {
        return "PageViewAccumulator{" +
                "f0=" + f0 +
                ", f1=" + f1 +
                ", f2=" + f2 +
                '}';
    }

    public PageViewAccumulator(String f0, Integer f1, Set<String> f2) {
        super(f0, f1, f2);
    }

    public String getKey() {
        return this.f0;
    }

    public void setKey(String key) {
        this.f0 = key;
    }

    public void addCount(int count) {
        setCount(this.f1 + count);
    }

    public int getCount() {
        return this.f1;
    }

    public void setCount(int count) {
        this.f1 = count;
    }

    public Set<String> getUserIds() {
        return this.f2;
    }

    public void addUserId(String userId) {
        this.f2.add(userId);
    }

    public void addUserIDs(Set<String> userIds) {
        this.f2.addAll(userIds);
    }

}
