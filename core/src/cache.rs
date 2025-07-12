use bytes::Bytes;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

struct Node<K, V> {
    k: K,
    v: V,
    next: Option<Arc<RwLock<Node<K, V>>>>,
    prev: Option<Arc<RwLock<Node<K, V>>>>,
}

impl<K, V> Node<K, V> {
    fn new(k: K, v: V) -> Self {
        Self {
            k,
            v,
            next: None,
            prev: None,
        }
    }
}

/// A thread-safe hash-map that uses lock striping.
///
/// Uses fixed sized buckets list.
struct ConcurrentHashMap<K, V> {
    buckets: Vec<Arc<RwLock<HashMap<K, V>>>>,
}

impl<K: Hash + Eq, V: Clone> ConcurrentHashMap<K, V> {
    /// returns a new `ConcurrentHashMap` with 16 buckets of hash-maps
    pub fn new() -> Self {
        Self {
            buckets: vec![Arc::new(RwLock::new(HashMap::new())); 16],
        }
    }

    /// gets the bucket (hash-map) where key `k` should be inserted
    pub fn get_bucket(k: &K) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        k.hash(&mut hasher);
        let b = hasher.finish() % 16;
        b
    }

    pub fn insert(&self, k: K, v: V) {
        let b = Self::get_bucket(&k);
        self.buckets[b as usize].write().unwrap().insert(k, v);
    }

    pub fn get(&self, k: &K) -> Option<V> {
        let b = Self::get_bucket(&k);
        let g = self.buckets[b as usize].read().unwrap();
        let v = g.get(&k);
        v.cloned() // should we return a ref or a clone?
    }

    pub fn remove(&self, k: &K) -> Option<V> {
        let b = Self::get_bucket(&k);
        self.buckets[b as usize].write().unwrap().remove(&k)
    }

    pub fn contains_key(&self, k: &K) -> bool {
        let b = Self::get_bucket(&k);
        self.buckets[b as usize].read().unwrap().contains_key(&k)
    }
}

struct ConcurrentLL<K, V> {
    inner: Arc<RwLock<ConcurrentLLInner<K, V>>>,
}

struct ConcurrentLLInner<K, V> {
    head: Option<Arc<RwLock<Node<K, V>>>>,
    tail: Option<Arc<RwLock<Node<K, V>>>>,
}

impl<K, V> ConcurrentLLInner<K, V> {
    fn new() -> Self {
        Self {
            head: None,
            tail: None,
        }
    }
}

impl<K: Eq + Hash + Clone, V: Debug + Clone> ConcurrentLL<K, V> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(ConcurrentLLInner::new())),
        }
    }
}

pub struct LruCache<K, V> {
    m: ConcurrentHashMap<K, Option<Arc<RwLock<Node<K, V>>>>>,
    ll: ConcurrentLL<K, V>,
    th: usize,
    len: AtomicUsize,
}

impl<K: Eq + Hash + Clone, V: Debug + Clone> LruCache<K, V> {
    /// creates a new `LruCache` with the given threshold `th`
    pub fn new(th: usize) -> Self {
        Self {
            m: ConcurrentHashMap::new(),
            ll: ConcurrentLL::new(),
            th,
            len: AtomicUsize::new(0),
        }
    }

    /// inserts value in `LruCache`, evicting lru entry if necessary
    pub fn insert(&self, k: K, v: V) {
        let new_node = Some(Arc::new(RwLock::new(Node::new(k.clone(), v))));

        // list empty
        if self.ll.inner.read().unwrap().head.is_none() {
            self.ll.inner.write().unwrap().head = new_node.clone();
            let head = self.ll.inner.read().unwrap().head.clone();
            self.ll.inner.write().unwrap().tail = head;
        } else {
            // check if threshold reached; evict head (lru)
            if self.len() == self.th {
                let head_k = self
                    .ll
                    .inner
                    .read()
                    .unwrap()
                    .head
                    .clone()
                    .unwrap()
                    .read()
                    .unwrap()
                    .k
                    .clone();
                // set head to next node
                let head_next = self
                    .ll
                    .inner
                    .read()
                    .unwrap()
                    .head
                    .clone()
                    .unwrap()
                    .read()
                    .unwrap()
                    .next
                    .clone();
                self.ll.inner.write().unwrap().head = head_next;
                // remove from map
                self.m.remove(&head_k);
                self.len.fetch_sub(1, Ordering::Relaxed);
            }
            self.ll
                .inner
                .write()
                .unwrap()
                .tail
                .clone()
                .unwrap()
                .write()
                .unwrap()
                .next = new_node.clone();
            new_node.clone().unwrap().write().unwrap().prev =
                self.ll.inner.read().unwrap().tail.clone();
            self.ll.inner.write().unwrap().tail = new_node.clone();
        }

        self.m.insert(k, new_node.clone());
        // todo abhi: check the ordering
        self.len.fetch_add(1, Ordering::Relaxed);
    }

    pub fn remove(&self, k: &K) -> Option<V>
    where
        K: Eq + Hash,
    {
        if let Some(n) = self.remove_internal(&k) {
            match Arc::try_unwrap(n) {
                Ok(n) => {
                    let n = n.into_inner();
                    Some(n.unwrap().v)
                }
                Err(_) => {
                    panic!("this shouldn't happen");
                }
            }
        } else {
            None
        }
    }

    /// removes an entry from the `LruCache`
    fn remove_internal(&self, k: &K) -> Option<Arc<RwLock<Node<K, V>>>>
    where
        K: Eq + Hash,
    {
        if self.m.contains_key(&k) {
            let node = self.m.get(&k).unwrap().clone();
            self.m.remove(&k);

            // todo abhi: check the ordering
            self.len.fetch_sub(1, Ordering::Relaxed);

            // tail node; del only prev & set tail to prev node
            if node.clone().unwrap().read().unwrap().next.is_none() {
                // set tail to prev node
                let mut inner = self.ll.inner.write().unwrap();
                if let Some(node_arc) = node.clone() {
                    let prev = node_arc.read().unwrap().prev.clone();
                    inner.tail = prev.clone();

                    if let Some(prev_arc) = prev {
                        prev_arc.write().unwrap().next = None;
                    }
                }
            } else if node.clone().unwrap().read().unwrap().prev.is_none() {
                // head node; remove next node's prev link, set head to next node
                self.ll.inner.write().unwrap().head =
                    node.clone().unwrap().read().unwrap().next.clone();
                self.ll
                    .inner
                    .write()
                    .unwrap()
                    .head
                    .clone()
                    .unwrap()
                    .write()
                    .unwrap()
                    .prev = None;
                node.clone().unwrap().write().unwrap().next = None;
            } else {
                // intermediate node; adjust prev & next pointers
                let prev = node.clone().unwrap().read().unwrap().prev.clone();
                let next = node.clone().unwrap().read().unwrap().next.clone();
                prev.clone().unwrap().write().unwrap().next = next.clone();
                next.clone().unwrap().write().unwrap().prev = prev.clone();
            }

            return node;
        }
        None
    }

    /// prints all entries in the `LruCache`
    fn print(&self) {
        let mut p = self.ll.inner.read().unwrap().head.clone();
        while let Some(n) = p {
            println!("{:?} ", n.read().unwrap().v);
            p = n.read().unwrap().next.clone();
        }
    }

    /// gets the value for the key `k` if present
    pub fn get(&self, k: &K) -> Option<V> {
        if self.m.contains_key(&k) {
            let node = self.remove_internal(&k)?;
            let mut inner = self.ll.inner.write().unwrap();
            if let Some(tail) = inner.tail.clone() {
                tail.write().unwrap().next = Some(node.clone());
            }

            inner.tail = Some(node.clone());

            Some(node.read().unwrap().v.clone())
        } else {
            None
        }
    }

    fn head(&self) -> V {
        self.ll
            .inner
            .read()
            .unwrap()
            .head
            .clone()
            .unwrap()
            .read()
            .unwrap()
            .v
            .clone()
    }

    fn tail(&self) -> V {
        self.ll
            .inner
            .read()
            .unwrap()
            .tail
            .clone()
            .unwrap()
            .read()
            .unwrap()
            .v
            .clone()
    }

    fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use bytes::Bytes;
    use crate::cache::{ConcurrentHashMap, LruCache};

    #[test]
    fn test() {
        let cache = LruCache::new(5);
        cache.insert(1, 1);
        cache.insert(2, 2);
        cache.insert(3, 3);
        assert_eq!(cache.head(), 1);
        assert_eq!(cache.tail(), 3);
        assert_eq!(cache.len(), 3);
        cache.print();
        cache.remove(&3);
        cache.print();
        assert_eq!(cache.tail(), 2);
        cache.insert(3, 3);
        cache.print();
        cache.remove(&2);
        cache.print();
        assert_eq!(cache.len(), 2);
        cache.remove(&1);
        assert_eq!(cache.len(), 1);
        cache.print();
    }

    #[test]
    fn test_threshold() {
        let cache = LruCache::new(5);
        cache.insert(1, 1);
        cache.insert(2, 2);
        cache.insert(3, 3);
        cache.insert(4, 4);
        cache.insert(5, 5);
        assert_eq!(cache.len(), 5);
        cache.insert(6, 6);
        assert_eq!(cache.len(), 5);
        assert_eq!(cache.head(), 2);
        assert_eq!(cache.tail(), 6);
    }

    #[test]
    fn test_accesses() {
        let cache = LruCache::new(5);
        cache.insert(1, 1);
        cache.insert(2, 2);
        cache.insert(3, 3);
        cache.insert(4, 4);
        cache.insert(5, 5);
        assert_eq!(cache.get(&3), Some(3));
        assert_eq!(cache.tail(), 3);
        assert_eq!(cache.get(&1), Some(1));
        assert_eq!(cache.tail(), 1);
        assert_eq!(cache.head(), 2);
    }

    #[test]
    fn test_generic() {
        let cache = LruCache::new(5);
        cache.insert(1, Bytes::from("abhi"));
        cache.insert(2, Bytes::from("ash"));
        cache.insert(3, Bytes::from("lilb"));
        cache.insert(4, Bytes::from("pads"));
        cache.insert(5, Bytes::from("zigg"));
        assert_eq!(cache.get(&3), Some(Bytes::from("lilb")));
        assert_eq!(cache.tail(), Bytes::from("lilb"));
        assert_eq!(cache.get(&1), Some(Bytes::from("abhi")));
        assert_eq!(cache.tail(), Bytes::from("abhi"));
        assert_eq!(cache.head(), Bytes::from("ash"));
    }

    #[test]
    fn test_hm() {
        let map = ConcurrentHashMap::new();
        map.insert(1, 1);
        map.insert(2, 2);
        map.insert(3, 3);
        map.insert(4, 4);
        map.insert(5, 5);
        map.insert(6, 6);
        map.insert(7, 7);
        assert_eq!(map.get(&1), Some(1));
        assert_eq!(map.get(&7), Some(7));
    }
    
    #[test]
    fn test_concurrent_ops() {
        use crossbeam_utils::thread;
        let cache = Arc::new(LruCache::new(5));
        thread::scope(|s| {
            for i in 1..=6 {
                let cache = cache.clone();
                s.spawn(move |_| {
                    cache.insert(i, i);
                });
            }
        }).unwrap();

        assert_eq!(cache.len(), 5);
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&4), Some(4));
    }
}