use chrono::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::prelude::*;
use std::io::Write;
use std::path::Path;
use uuid::Uuid;

const ACTION_ADD: &str = "A";
const ACTION_DELETE: &str = "D";
const ACTION_UPDATE: &str = "U";

#[derive(Clone)]
struct Record {
  id: String,
  action: String,
  content: String,
}

struct Topic {
  path: String,
  id: String,
  line_map: HashMap<usize, String>,
  record_map: HashMap<String, Record>,
}

impl Topic {
  pub fn new(topic_id: &str, topic_path: &str) -> Topic {
    let mut topic = Topic {
      path: topic_path.to_string(),
      id: topic_id.to_string(),
      line_map: HashMap::new(),
      record_map: HashMap::new(),
    };
    let records = Topic::get_records(topic_path);
    for record in records {
      if record.action == ACTION_DELETE {
        if topic.record_map.contains_key(&record.id) {
          topic.record_map.remove(&record.id);
        }
      } else {
        topic.record_map.insert(record.id.clone(), record.clone());
      }
    }
    let mut index = 1;
    for value in topic.record_map.values() {
      topic.line_map.insert(index, value.id.clone());
      index += 1;
    }

    return topic;
  }

  fn get_records(path: &str) -> Vec<Record> {
    let mut file = OpenOptions::new().read(true).open(path).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let mut lines: Vec<&str> = contents.split('\n').collect();
    lines.pop();
    let mut records: Vec<Record> = Vec::new();
    for line in lines {
      let record = Record {
        id: line[0..36].to_string(),
        action: line[36..37].to_string(),
        content: line[37..].to_string(),
      };
      records.push(record);
    }
    return records;
  }

  fn display_prompt(topic_id: &str) {
    print!("({})> ", topic_id);
    io::stdout().flush().expect("Failed to flush stdout");
  }

  fn read_line() -> String {
    let mut input = String::new();
    io::stdin()
      .read_line(&mut input)
      .expect("Failed to read line");
    input.trim().to_string()
  }

  fn get_confirmation() -> bool {
    print!("y/n ? ");
    io::stdout().flush().expect("Failed to flush stdout");
    let response = Topic::read_line();
    response == "Y" || response == "y"
  }

  fn append_data(&self, record: &Record) {
    let output = format!("{}{}{}\n", record.id, record.action, record.content);
    let mut file = OpenOptions::new().append(true).open(&self.path).unwrap();
    file.write_all(output.as_bytes()).expect("Add failed");
  }

  fn add(&mut self, args: &[&str]) {
    if args.len() == 0 {
      println!("Nothing to add");
      return;
    }
    let output = args.join(" ");
    let id = Uuid::new_v4();
    let record = Record {
      id: id.to_string(),
      action: ACTION_ADD.to_string(),
      content: output,
    };
    self.append_data(&record);
    let index = self.line_map.len() + 1;
    self.line_map.insert(index, record.id.clone());
    self.record_map.insert(record.id.clone(), record.clone());
  }

  fn delete(&mut self, args: &[&str]) {
    if args.len() == 0 {
      println!("Nothing to delete. Line number required");
      return;
    }
    let index = args[0].to_string().parse::<usize>().unwrap();
    let record = &self.line_map.get(&index);
    if record.is_some() {
      let selected_record = record.unwrap();
      let record_value = self.record_map.get(selected_record).unwrap();
      let content = record_value.content.clone();
      println!("Confirm deletion of \"{}\"", content);
      let confirmed = Topic::get_confirmation();
      if confirmed {
        let deleted_record = Record {
          id: selected_record.clone(),
          action: ACTION_DELETE.to_string(),
          content: "-".to_string(),
        };
        self.append_data(&deleted_record);
        self
          .record_map
          .insert(deleted_record.id.clone(), deleted_record.clone());
        println!("\"{}\" deleted", content);
      }
    } else {
      println!("No item found at position {}", index);
    }
  }

  fn update(&mut self, args: &[&str]) {
    if args.len() == 0 {
      println!("Nothing to delete. Line number required");
      return;
    }
    let index = args[0].to_string().parse::<usize>().unwrap();
    let record = &self.line_map.get(&index);
    if record.is_some() {
      let selected_record = record.unwrap();
      let record_value = self.record_map.get(selected_record).unwrap();
      let content = record_value.content.clone();
      println!("Update \"{}\"", content);
      print!("new value ? ");
      io::stdout().flush().expect("Failed to flush stdout");
      let new_value = Topic::read_line();
      if !new_value.to_string().is_empty() {
        let updated_record = Record {
          id: selected_record.clone(),
          action: ACTION_UPDATE.to_string(),
          content: new_value.to_string(),
        };
        self.append_data(&updated_record);
        self
          .record_map
          .insert(updated_record.id.clone(), updated_record.clone());
        println!("\"{}\" updated to \"{}\"", content, updated_record.content);
      }
    } else {
      println!("No item found at position {}", index);
    }
  }

  fn list(&self) {
    println!("----------------------------------------------");
    let record_count = self.record_map.len();
    for index in 1..record_count + 1 {
      let id = self.line_map.get(&index);
      if id.is_some() {
        let record_id = id.unwrap();
        let record = self.record_map.get(record_id);
        if record.is_some() {
          let record_value = record.unwrap();
          if record_value.action != ACTION_DELETE {
            println!("{}: {}", index, record_value.content);
          }
        }
      }
    }
    println!("----------------------------------------------");
  }

  fn refresh(&mut self) {
    self.line_map.clear();
    self.record_map.clear();

    let records = Topic::get_records(&self.path);
    for record in records {
      if record.action == ACTION_DELETE {
        if self.record_map.contains_key(&record.id) {
          self.record_map.remove(&record.id);
        }
      } else {
        self.record_map.insert(record.id.clone(), record.clone());
      }
    }
    let mut index = 1;
    for value in self.record_map.values() {
      self.line_map.insert(index, value.id.clone());
      index += 1;
    }
  }

  fn compact(&mut self) {
    let time_stamp: DateTime<Local> = Local::now();
    let move_path = format!("{}.bkp_{}", self.path, time_stamp.format("%Y%m%d_%H%M%S%f"));
    fs::rename(&self.path, &move_path);
    File::create(&self.path);
    for record in self.record_map.values() {
      self.append_data(&record);
    }
    self.refresh();
  }

  fn open(&mut self) {
    loop {
      Topic::display_prompt(&self.id);
      let line = Topic::read_line();
      let command_line: Vec<&str> = line.split(' ').collect();
      let command: &str = &command_line[0].to_string().trim().to_uppercase();
      match command {
        "CLOSE" => break,
        "ADD" => self.add(&command_line[1..]),
        "DELETE" => self.delete(&command_line[1..]),
        "UPDATE" => self.update(&command_line[1..]),
        "LIST" => self.list(),
        "REFRESH" => self.refresh(),
        _ => println!("Not a valid command"),
      }
    }
  }
}

/// Manages topics in the database
pub struct Topics {
  /// Location of the database
  pub db_home: String,
}

impl Topics {
  /// Creates a topic in the database. The name of the topic must be
  /// unique. If the topic already exists, a new topic will not be created.
  ///
  /// # Arguments
  ///
  /// * `topic_id` - The name of the new topic.
  pub fn create(&self, topic_id: &str) {
    if self.topic_exists(&topic_id) {
      //TODO Should return a status, not implement a side effect.
      println!("The topic {} already exists.", topic_id);
      return;
    }
    match File::create(self.topic_path(topic_id)) {
      //TODO Should return a status, not implement a side effect.
      Ok(_) => println!("Topic {} created.", topic_id),
      Err(_) => println!("Error occured creating topic {}", topic_id),
    }
  }

  pub fn list(&self) -> Vec<String> {
    let mut items: Vec<String> = Vec::new();
    let files = fs::read_dir(self.db_home.clone()).unwrap();
    for file in files {
      let path = file.unwrap().path();
      let topic_name = path.file_stem().unwrap().to_str().unwrap();
      let topic_type = path.extension().unwrap().to_str().unwrap();
      if topic_type == "tpc" {
        items.push(topic_name.to_string());
      }
    }
    items
  }

  fn topic_path(&self, topic_id: &str) -> String {
    format!("{}\\{}.tpc", self.db_home, topic_id)
  }

  fn topic_exists(&self, topic_id: &str) -> bool {
    let topic_path = self.topic_path(topic_id);
    Path::new(&topic_path).exists()
  }

  pub fn open(&self, topic_id: &str) {
    if !self.topic_exists(&topic_id) {
      println!("{} does not exist.", topic_id);
      return;
    }
    let topic_path = self.topic_path(topic_id);
    let mut topic = Topic::new(topic_id, &topic_path);
    topic.open();
  }

  pub fn compact(&self, topic_id: &str) {
    if !self.topic_exists(&topic_id) {
      println!("{} does not exist.", topic_id);
      return;
    }
    let topic_path = self.topic_path(topic_id);
    let mut topic = Topic::new(topic_id, &topic_path);
    topic.compact();
  }
}
