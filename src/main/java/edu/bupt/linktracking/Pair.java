package edu.bupt.linktracking;

public class Pair<K, L> {
    public K first;
    public L second;

    public Pair() {
    }

    public Pair(K first, L second) {
        this.first = first;
        this.second = second;
    }

    public String toString() {
        return first + ":" + second;
    }
}
