use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::rc::Rc;

struct Node<K, V> {
    k: K,
    v: V,
    next: Option<Rc<RefCell<Node<K, V>>>>,
    prev: Option<Rc<RefCell<Node<K, V>>>>,
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

struct LruCache<K, V> {
    m: HashMap<K, Option<Rc<RefCell<Node<K, V>>>>>,
    head: Option<Rc<RefCell<Node<K, V>>>>,
    tail: Option<Rc<RefCell<Node<K, V>>>>,
    th: usize,
}

impl<K: Eq + Hash + Clone, V: Display + Clone> LruCache<K, V> {
    /// creates a new `LruCache` with the given threshold `th`
    fn new(th: usize) -> Self {
        LruCache {
            m: HashMap::new(),
            head: None,
            tail: None,
            th,
        }
    }

    /// inserts value in `LruCache`, evicting lru entry if necessary
    fn insert(&mut self, k: K, v: V) {
        let new_node = Some(Rc::new(RefCell::new(Node::new(k.clone(), v))));

        // list empty
        if self.head.is_none() {
            self.head = new_node.clone();
            self.tail = self.head.clone();
        } else {
            // check if threshold reached; evict head (lru)
            if self.len() == self.th {
                let head_k = self.head.clone().unwrap().borrow().k.clone();
                // set head to next node
                self.head = self.head.clone().unwrap().borrow().next.clone();
                // remove from map
                self.m.remove(&head_k);
            }
            self.tail.clone().unwrap().borrow_mut().next = new_node.clone();
            new_node.clone().unwrap().borrow_mut().prev = self.tail.clone();
            self.tail = new_node.clone();
        }

        self.m.insert(k, new_node.clone());
    }

    fn remove(&mut self, k: K) -> Option<V> where
    K: Eq + Hash {
        if let Some(n) = self.remove_internal(k) {
            match Rc::try_unwrap(n) {
                Ok(n) => {
                    let n = n.into_inner();
                    Some(n.v)
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
    fn remove_internal(&mut self, k: K) -> Option<Rc<RefCell<Node<K, V>>>> where
    K: Eq + Hash {
        if self.m.contains_key(&k) {
            let node = self.m.get(&k).unwrap().clone();
            self.m.remove(&k);

            // tail node; del only prev & set tail to prev node
            if node.clone().unwrap().borrow().next.is_none() {
                // set tail to prev node
                self.tail = node.clone().unwrap().borrow().prev.clone();
                // disconnect new tail from the last node
                self.tail.clone().unwrap().borrow_mut().next = None;
            } else if node.clone().unwrap().borrow().prev.is_none() {
                // head node; remove next node's prev link, set head to next node
                self.head = node.clone().unwrap().borrow().next.clone();
                self.head.clone().unwrap().borrow_mut().prev = None;
                node.clone().unwrap().borrow_mut().next = None;
            } else {
                // intermediate node; adjust prev & next pointers
                let prev = node.clone().unwrap().borrow().prev.clone();
                let next = node.clone().unwrap().borrow().next.clone();
                prev.clone().unwrap().borrow_mut().next = next.clone();
                next.clone().unwrap().borrow_mut().prev = prev.clone();
            }

            return node;
        }
        None
    }

    /// prints all entries in the `LruCache`
    fn print(&self) {
        let mut p = self.head.clone();
        while let Some(n) = p {
            println!("{} ", n.borrow().v);
            p = n.borrow().next.clone();
        }
    }

    /// gets the value for the key `k` if present
    fn get(&mut self, k: K) -> Option<V> {
        if self.m.contains_key(&k) {
            let node = self.remove_internal(k);
            self.tail.clone().unwrap().borrow_mut().next = node.clone();
            self.tail = node.clone();
            Some(node.clone().unwrap().borrow().v.clone())
        } else {
            None
        }
    }

    fn head(&self) -> V {
        self.head.clone().unwrap().borrow().v.clone()
    }

    fn tail(&self) -> V {
        self.tail.clone().unwrap().borrow().v.clone()
    }

    fn len(&self) -> usize {
        self.m.len()
    }
}

#[test]
fn test() {
    let mut cache = LruCache::new(5);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.insert(3, 3);
    assert_eq!(cache.head(), 1);
    assert_eq!(cache.tail(), 3);
    assert_eq!(cache.len(), 3);
    cache.print();
    cache.remove(3);
    cache.print();
    assert_eq!(cache.tail(), 2);
    cache.insert(3, 3);
    cache.print();
    cache.remove(2);
    cache.print();
    assert_eq!(cache.len(), 2);
    cache.remove(1);
    assert_eq!(cache.len(), 1);
    cache.print();
}

#[test]
fn test_threshold() {
    let mut cache = LruCache::new(5);
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
    let mut cache = LruCache::new(5);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.insert(3, 3);
    cache.insert(4, 4);
    cache.insert(5, 5);
    assert_eq!(cache.get(3), Some(3));
    assert_eq!(cache.tail(), 3);
    assert_eq!(cache.get(1), Some(1));
    assert_eq!(cache.tail(), 1);
    assert_eq!(cache.head(), 2);
}
