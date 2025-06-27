use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn parse_input(input: &str) -> Command {
    let input_array: Vec<&str> = input.trim().split(' ').collect();
    match input_array[0] {
        "set" => {
            if input_array.len() == 5 {
                return Command::Set(
                    input_array[1].to_string(),
                    input_array[2].parse().unwrap(),
                    input_array[3].parse().unwrap(),
                    Bytes::from(input_array[4].as_bytes().to_owned()),
                );
            }
        }
        "get" => {
            if input_array.len() == 2 {
                return Command::Get(input_array[1].to_string());
            }
        }
        "delete" => {
            if input_array.len() == 2 {
                return Command::Delete(input_array[1].to_string());
            }
        }
        "version" => {
            if input_array.len() == 1 {
                return Command::Version;
            }
        }
        _ => {}
    }

    Command::Invalid
}

#[derive(Debug, PartialEq)]
pub enum Command {
    // set <key> <flags> <exptime> <data>
    Set(String, usize, u128, Bytes),
    Add(String, Bytes),
    Replace(String, Bytes),
    Append(String, Bytes),
    Prepend(String, Bytes),
    Get(String),
    Gets(String),
    Delete(String),
    Incr(String),
    Decr(String),
    Cas(String, Bytes),
    Stats,
    Version,
    Flushall,
    Invalid,
}

impl Command {
    pub fn handle(self, map: Arc<DashMap<String, (u128, Bytes)>>) -> anyhow::Result<Bytes> {
        match self {
            Command::Set(key, flags, exp_time, data) => {
                let exp_time = if exp_time != 0 {
                    SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() + exp_time
                } else {
                    0
                };
                map.insert(key, (exp_time, data));
                Ok(Bytes::from("STORED"))
            }
            Command::Add(_, _) => Ok(Bytes::from("NOT IMPLEMENTED")),
            Command::Replace(_, _) => Ok(Bytes::from("NOT IMPLEMENTED")),
            Command::Append(_, _) => Ok(Bytes::from("NOT IMPLEMENTED")),
            Command::Prepend(_, _) => Ok(Bytes::from("NOT IMPLEMENTED")),
            Command::Get(key) => {
                if let Some(v) = map.get(&key) {
                    if v.0 != 0 {
                        let current_time =
                            SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
                        if v.0 >= current_time {
                            Ok(v.1.clone())
                        } else {
                            Ok(Bytes::from("NOT FOUND"))
                        }
                    } else {
                        Ok(v.1.clone())
                    }
                } else {
                    Ok(Bytes::from("NOT FOUND"))
                }
            }
            Command::Gets(_) => Ok(Bytes::from("NOT IMPLEMENTED")),
            Command::Delete(key) => {
                if let Some(_) = map.remove(&key) {
                    Ok(Bytes::from("DELETED"))
                } else {
                    Ok(Bytes::from("NOT FOUND"))
                }
            }
            Command::Incr(_) => Ok(Bytes::from("NOT IMPLEMENTED")),
            Command::Decr(_) => Ok(Bytes::from("NOT IMPLEMENTED")),
            Command::Cas(_, _) => Ok(Bytes::from("NOT IMPLEMENTED")),
            Command::Stats => Ok(Bytes::from("NOT IMPLEMENTED")),
            Command::Version => Ok(Bytes::from(env!("CARGO_PKG_VERSION"))),
            Command::Flushall => Ok(Bytes::from("NOT IMPLEMENTED")),
            Command::Invalid => Ok(Bytes::from("NOT IMPLEMENTED")),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::{Command, parse_input};
    use bytes::Bytes;
    use dashmap::DashMap;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_parse_input() {
        let cmd = parse_input("set abhi 0 200 rust");
        assert_eq!(
            cmd,
            Command::Set("abhi".to_string(), 0, 200, Bytes::from("rust"))
        );

        let cmd = parse_input("get abhi");
        assert_eq!(cmd, Command::Get("abhi".to_string()));

        let cmd = parse_input("delete abhi");
        assert_eq!(cmd, Command::Delete("abhi".to_string()));

        let cmd = parse_input("blah abhi");
        assert_eq!(cmd, Command::Invalid);
    }

    #[test]
    fn test_storage() {
        let store = Arc::new(DashMap::new());
        let out = parse_input("set abhi 0 200 rust")
            .handle(store.clone())
            .unwrap();
        assert_eq!(Bytes::from("STORED"), out);

        let out = parse_input("get abhi").handle(store.clone()).unwrap();
        assert_eq!(Bytes::from("rust"), out);

        let out = parse_input("set abhi 0 200 c++")
            .handle(store.clone())
            .unwrap();
        assert_eq!(Bytes::from("STORED"), out);

        let out = parse_input("get abhi").handle(store.clone()).unwrap();
        assert_eq!(Bytes::from("c++"), out);

        let out = parse_input("set abhi 0 0 python")
            .handle(store.clone())
            .unwrap();
        assert_eq!(Bytes::from("STORED"), out);
        let out = parse_input("get abhi").handle(store.clone()).unwrap();
        assert_eq!(Bytes::from("python"), out);

        let out = parse_input("set abhi 0 200 java")
            .handle(store.clone())
            .unwrap();
        assert_eq!(Bytes::from("STORED"), out);
        thread::sleep(Duration::from_millis(100));
        let out = parse_input("get abhi").handle(store.clone()).unwrap();
        assert_eq!(Bytes::from("java"), out);
    }

    #[test]
    fn test_expiry() {
        let store = Arc::new(DashMap::new());
        let out = parse_input("set abhi 0 200 kotlin")
            .handle(store.clone())
            .unwrap();
        assert_eq!(Bytes::from("STORED"), out);
        thread::sleep(Duration::from_millis(300));
        let out = parse_input("get abhi").handle(store.clone()).unwrap();
        assert_eq!(Bytes::from("NOT FOUND"), out);
    }
}
