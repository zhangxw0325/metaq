package com.taobao.metamorphosis.server.utils;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * 来自于mina项目<a href="http://mina.apache.org">Apache MINA Project</a>
 * 
 * @author 无花
 * @since 2011-8-11 下午3:08:09
 */

public class CopyOnWriteMap<K, V> implements Map<K, V>, Cloneable, Serializable {
    private static final long serialVersionUID = 788933834504546710L;

    private volatile Map<K, V> internalMap;


    public CopyOnWriteMap() {
        this.internalMap = new HashMap<K, V>();
    }


    public CopyOnWriteMap(int initialCapacity) {
        this.internalMap = new HashMap<K, V>(initialCapacity);
    }


    public CopyOnWriteMap(Map<K, V> data) {
        this.internalMap = new HashMap<K, V>(data);
    }


    public V put(K key, V value) {
        synchronized (this) {
            Map<K, V> newMap = new HashMap<K, V>(this.internalMap);
            V val = newMap.put(key, value);
            this.internalMap = newMap;
            return val;
        }
    }


    public V remove(Object key) {
        synchronized (this) {
            Map<K, V> newMap = new HashMap<K, V>(this.internalMap);
            V val = newMap.remove(key);
            this.internalMap = newMap;
            return val;
        }
    }


    public void putAll(Map<? extends K, ? extends V> newData) {
        synchronized (this) {
            Map<K, V> newMap = new HashMap<K, V>(this.internalMap);
            newMap.putAll(newData);
            this.internalMap = newMap;
        }
    }


    public void clear() {
        synchronized (this) {
            this.internalMap = new HashMap<K, V>();
        }
    }


    public int size() {
        return this.internalMap.size();
    }


    public boolean isEmpty() {
        return this.internalMap.isEmpty();
    }


    public boolean containsKey(Object key) {
        return this.internalMap.containsKey(key);
    }


    public boolean containsValue(Object value) {
        return this.internalMap.containsValue(value);
    }


    public V get(Object key) {
        return this.internalMap.get(key);
    }


    public Set<K> keySet() {
        return this.internalMap.keySet();
    }


    public Collection<V> values() {
        return this.internalMap.values();
    }


    public Set<Entry<K, V>> entrySet() {
        return this.internalMap.entrySet();
    }


    @Override
    public Object clone() {
        try {
            return super.clone();
        }
        catch (CloneNotSupportedException e) {
            throw new InternalError();
        }
    }
}