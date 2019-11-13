use crate::dbprocess::ContextProcess;
use crate::dbprocess::DBResponse;
use chrono::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
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
  record_map: HashMap<String, Record>,
}

impl Topic {
  pub fn new(topic_id: &str, topic_path: &str) -> Topic {
    let mut topic = Topic {
      path: topic_path.to_string(),
      id: topic_id.to_string(),
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

  fn append_data(&self, record: &Record) {
    let output = format!("{}{}{}\n", record.id, record.action, record.content);
    let mut file = OpenOptions::new().append(true).open(&self.path).unwrap();
    file.write_all(output.as_bytes()).expect("Add failed");
  }

  fn add(&mut self, args: &[&str]) -> DBResponse<(Box<dyn ContextProcess>, String)> {
    if args.len() == 0 {
      return DBResponse::Invalid("Content for ADD cannot be empty.".to_string());
    }
    let output = args.join(" ");
    let id = Uuid::new_v4();
    let record = Record {
      id: id.to_string(),
      action: ACTION_ADD.to_string(),
      content: output,
    };
    self.append_data(&record);
    self.record_map.insert(record.id.clone(), record.clone());
    DBResponse::ROk("Content added.".to_string())
  }

  fn delete(&mut self, args: &[&str]) -> DBResponse<(Box<dyn ContextProcess>, String)> {
    if args.len() == 0 {
      return DBResponse::Invalid("DELETE requires a key".to_string());
    }
    let selected_record = args[0];
    let record_value = self.record_map.get(selected_record).unwrap();
    let content = record_value.content.clone();
    let deleted_record = Record {
      id: selected_record.to_string(),
      action: ACTION_DELETE.to_string(),
      content: "-".to_string(),
    };
    self.append_data(&deleted_record);
    self
      .record_map
      .insert(deleted_record.id.clone(), deleted_record.clone());
    let message = format!("\"{}\" deleted", content);
    return DBResponse::ROk(message.to_string());
  }

  fn update(&mut self, args: &[&str]) -> DBResponse<(Box<dyn ContextProcess>, String)> {
    if args.len() < 2 {
      return DBResponse::Invalid("UPDATE requires a key and an updated value".to_string());
    }
    let selected_record = args[0];
    let content = args[1..].join(" ");
    let record_value = self.record_map.get(selected_record).unwrap();
    let original_content = record_value.content.to_string();
    let updated_record = Record {
      id: selected_record.to_string(),
      action: ACTION_UPDATE.to_string(),
      content: content.to_string(),
    };
    self.append_data(&updated_record);
    self
      .record_map
      .insert(updated_record.id.clone(), updated_record.clone());
    let message = format!("\"{}\" updated to \"{}\"", original_content, content);
    DBResponse::ROk(message)
  }

  fn list(&self) -> DBResponse<(Box<dyn ContextProcess>, String)> {
    let mut list: Vec<(String, String)> = Vec::new();
    for record in self.record_map.values() {
      if record.action != ACTION_DELETE {
        list.push((record.id.to_string(), record.content.to_string()));
      }
    }
    DBResponse::Data(list)
  }

  fn refresh(&mut self) -> DBResponse<(Box<dyn ContextProcess>, String)> {
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
    DBResponse::ROk("Topic refreshed.".to_string())
  }

  fn compact(&mut self) -> DBResponse<(Box<dyn ContextProcess>, String)> {
    let time_stamp: DateTime<Local> = Local::now();
    let move_path = format!("{}.bkp_{}", self.path, time_stamp.format("%Y%m%d_%H%M%S%f"));
    match fs::rename(&self.path, &move_path) {
      Ok(_) => match File::create(&self.path) {
        Ok(_) => {
          for record in self.record_map.values() {
            self.append_data(&record);
          }
          self.refresh();
          DBResponse::ROk("Topic compacted.".to_string())
        }
        Err(_) => {
          DBResponse::Error("An error occured while backing up the original file.".to_string())
        }
      },
      Err(_) => {
        DBResponse::Error("An error occured while backing up the original file.".to_string())
      }
    }
  }
}

impl ContextProcess for Topic {
  fn id(&self) -> String {
    self.id.to_string()
  }

  fn process(&mut self, request: &str) -> DBResponse<(Box<dyn ContextProcess>, String)> {
    let command_line: Vec<&str> = request.split(' ').collect();
    let command: &str = &command_line[0].to_string().trim().to_uppercase();
    match command {
      "CLOSE" => DBResponse::CloseContext,
      "ADD" => self.add(&command_line[1..]),
      "DELETE" => self.delete(&command_line[1..]),
      "UPDATE" => self.update(&command_line[1..]),
      "LIST" => self.list(),
      "REFRESH" => self.refresh(),
      _ => DBResponse::Unknown,
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
  pub fn create(&self, topic_id: &str) -> Result<String, String> {
    if self.topic_exists(&topic_id) {
      let message = format!("The topic {} already exists.", topic_id);
      return Err(message);
    }
    match File::create(self.topic_path(topic_id)) {
      Ok(_) => {
        let message = format!("Topic {} created.", topic_id);
        Ok(message)
      }
      Err(_) => {
        let message = format!("Error occured creating topic {}", topic_id);
        Err(message)
      }
    }
  }

  pub fn list(&self) -> Vec<(String, String)> {
    let mut items: Vec<(String, String)> = Vec::new();
    let files = fs::read_dir(self.db_home.clone()).unwrap();
    for file in files {
      let path = file.unwrap().path();
      let topic_name = path.file_stem().unwrap().to_str().unwrap();
      let topic_type = path.extension().unwrap().to_str().unwrap();
      if topic_type == "tpc" {
        items.push(("".to_string(), topic_name.to_string()));
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

  pub fn open(&self, topic_id: &str) -> DBResponse<(Box<dyn ContextProcess>, String)> {
    if !self.topic_exists(&topic_id) {
      let message = format!("{} does not exist.", topic_id);
      return DBResponse::Error(message);
    }
    let topic_path = self.topic_path(topic_id);
    let topic = Topic::new(topic_id, &topic_path);
    DBResponse::OpenContext((Box::new(topic), topic_id.to_string()))
  }

  pub fn compact(&self, topic_id: &str) -> DBResponse<(Box<dyn ContextProcess>, String)> {
    if !self.topic_exists(&topic_id) {
      let message = format!("{} does not exist.", topic_id);
      return DBResponse::Invalid(message.to_string());
    }
    let topic_path = self.topic_path(topic_id);
    let mut topic = Topic::new(topic_id, &topic_path);
    topic.compact()
  }
}
