use crate::dbprocess::ContextController;
use crate::dbprocess::ContextProcess;
use crate::dbprocess::DBResponse;
use crate::topics::TopicController;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(PartialEq, Eq, Hash)]
enum Target {
  Topic,
  Directory,
  None,
}

struct Request {
  command: String,
  target: Target,
  arguments: Option<String>,
}

/// Manages directories in the database
pub struct DirectoryController {
  /// Location of the database
  pub db_home: String,
  pub relative_path: String,
}

impl DirectoryController {
  pub fn new(db_home: &str, relative_path: &str) -> DirectoryController {
    DirectoryController {
      db_home: db_home.to_string(),
      relative_path: relative_path.to_string(),
    }
  }

  fn directory_path(&self, directory_id: &str) -> String {
    format!("{}\\{}", self.db_home, directory_id)
  }

  fn directory_exists(&self, directory_id: &str) -> bool {
    let directory_path = self.directory_path(directory_id);
    Path::new(&directory_path).exists()
  }
}

pub struct DirectoryContext {
  pub db_home: String,
  pub relative_path: String,
  controller_map: HashMap<Target, Box<dyn ContextController>>,
}

impl DirectoryContext {
  pub fn new(db_home: &str, relative_path: &str) -> DirectoryContext {
    let mut directory_context = DirectoryContext {
      db_home: db_home.to_string(),
      relative_path: relative_path.to_string(),
      controller_map: HashMap::new(),
    };
    directory_context.controller_map.insert(
      Target::Topic,
      Box::new(TopicController::new(&db_home, &relative_path)),
    );
    directory_context.controller_map.insert(
      Target::Directory,
      Box::new(DirectoryController::new(&db_home, &relative_path)),
    );
    directory_context
  }

  fn parse_request(request: &str) -> Result<Request, &'static str> {
    if request.len() == 0 {
      return Err("nothing to parse");
    }
    let tokens: Vec<&str> = request.split(' ').collect();
    let command = tokens.get(0).unwrap();
    if tokens.len() == 1 {
      return Ok(Request {
        command: command.to_uppercase(),
        target: Target::None,
        arguments: None,
      });
    }
    let target_token = tokens.get(1).unwrap();
    let target = target_token.to_uppercase();
    let arguments = if tokens.len() > 2 {
      Some(tokens[2..].join(" "))
    } else {
      None
    };
    match target.as_str() {
      "TOPIC" => {
        return Ok(Request {
          command: command.to_uppercase(),
          target: Target::Topic,
          arguments: arguments,
        })
      }
      "DIRECTORY" => {
        return Ok(Request {
          command: command.to_uppercase(),
          target: Target::Directory,
          arguments: arguments,
        })
      }
      _ => {
        return Ok(Request {
          command: command.to_uppercase(),
          target: Target::None,
          arguments: Some(tokens[1..].join(" ")),
        })
      }
    }
  }

  fn list(&self, request: &Request) -> DBResponse<(Box<dyn ContextProcess>, String)> {
    match self.controller_map.get(&request.target) {
      Some(controller) => {
        let list = controller.list();
        DBResponse::Data(list)
      }
      None => DBResponse::Invalid(
        "Valid type required. (expected \"TOPIC\" or \"DIRECTORY\")".to_string(),
      ),
    }
  }

  fn create(&self, request: &Request) -> DBResponse<(Box<dyn ContextProcess>, String)> {
    match self.controller_map.get(&request.target) {
      Some(controller) => match &request.arguments {
        Some(arguments) => match controller.create(&arguments) {
          Ok(message) => DBResponse::ROk(message.to_string()),
          Err(message) => DBResponse::Error(message.to_string()),
        },
        _ => DBResponse::Invalid("Create requires an id".to_string()),
      },
      None => DBResponse::Invalid(
        "Valid type required. (expected \"TOPIC\" or \"DIRECTORY\")".to_string(),
      ),
    }
  }

  fn status(&self) -> DBResponse<(Box<dyn ContextProcess>, String)> {
    let mut items: Vec<(String, String)> = Vec::new();
    let path = self.db_home.clone();
    let property = format!("database.home: {}", path);
    items.push(("".to_string(), property.to_string()));
    DBResponse::Data(items)
  }

  fn drop(&self, request: &Request) -> DBResponse<(Box<dyn ContextProcess>, String)> {
    match self.controller_map.get(&request.target) {
      Some(controller) => match &request.arguments {
        Some(arguments) => match controller.drop_item(&arguments) {
          Ok(message) => DBResponse::ROk(message.to_string()),
          Err(message) => DBResponse::Error(message.to_string()),
        },
        _ => DBResponse::Invalid("Drop requires an id".to_string()),
      },
      None => DBResponse::Invalid(
        "Valid type required. (expected \"TOPIC\" or \"DIRECTORY\")".to_string(),
      ),
    }
  }

  fn open(&self, request: &Request) -> DBResponse<(Box<dyn ContextProcess>, String)> {
    match self.controller_map.get(&request.target) {
      Some(controller) => match &request.arguments {
        Some(arguments) => controller.open(&arguments),
        _ => DBResponse::Invalid("Open requires an id".to_string()),
      },
      None => DBResponse::Invalid(
        "Valid type required. (expected \"TOPIC\" or \"DIRECTORY\")".to_string(),
      ),
    }
  }

  fn compact(&self, request: &Request) -> DBResponse<(Box<dyn ContextProcess>, String)> {
    match self.controller_map.get(&request.target) {
      Some(controller) => match &request.arguments {
        Some(arguments) => controller.compact(&arguments),
        _ => DBResponse::Invalid("Compact requires an id".to_string()),
      },
      None => DBResponse::Invalid(
        "Valid type required. (expected \"TOPIC\" or \"DIRECTORY\")".to_string(),
      ),
    }
  }
}

impl ContextProcess for DirectoryContext {
  fn id(&self) -> String {
    self.relative_path.to_string()
  }

  fn process(&mut self, request_text: &str) -> DBResponse<(Box<dyn ContextProcess>, String)> {
    match DirectoryContext::parse_request(request_text) {
      Ok(parsed) => match parsed.command.as_str() {
        "LIST" => self.list(&parsed),
        "STATUS" => self.status(),
        "CREATE" => self.create(&parsed),
        "OPEN" => self.open(&parsed),
        "COMPACT" => self.compact(&parsed),
        "DROP" => self.drop(&parsed),
        "EXIT" => DBResponse::Exit,
        _ => DBResponse::Unknown(parsed.command),
      },
      Err(message) => return DBResponse::Error(message.to_string()),
    }
  }
}

impl ContextController for DirectoryController {
  fn create(&self, directory_id: &str) -> Result<String, String> {
    if self.directory_exists(&directory_id) {
      let message = format!("The directory {} already exists.", directory_id);
      return Err(message);
    }
    match fs::create_dir(self.directory_path(directory_id)) {
      Ok(_) => {
        let message = format!("Directory {} created.", directory_id);
        Ok(message)
      }
      Err(_) => {
        let message = format!("Error occured creating directory {}", directory_id);
        Err(message)
      }
    }
  }

  fn list(&self) -> Vec<(String, String)> {
    let mut items: Vec<(String, String)> = Vec::new();
    let current_dir = format!("{}{}", self.db_home, self.relative_path);
    let files = fs::read_dir(current_dir).unwrap();
    for file_result in files {
      let file = file_result.unwrap();
      let metadata = file.metadata();
      if metadata.unwrap().is_dir() {
        let path = file.path();
        let dir_name = path.file_stem().unwrap().to_str().unwrap();
        items.push(("".to_string(), dir_name.to_string()));
      }
    }
    items
  }

  fn drop_item(&self, directory_id: &str) -> Result<String, String> {
    if !self.directory_exists(&directory_id) {
      let message = format!("The directory {} does not exist.", directory_id);
      return Err(message);
    }
    match fs::remove_file(self.directory_path(directory_id)) {
      Ok(_) => {
        let message = format!("Topic {} dropped.", directory_id);
        Ok(message)
      }
      Err(_) => {
        let message = format!("Error occured dropping topic {}", directory_id);
        Err(message)
      }
    }
  }

  fn open(&self, directory_id: &str) -> DBResponse<(Box<dyn ContextProcess>, String)> {
    if !self.directory_exists(&directory_id) {
      let message = format!("{} does not exist.", directory_id);
      return DBResponse::Error(message);
    }
    let new_path = format!("{}\\{}", self.relative_path, directory_id);
    let directory = DirectoryContext::new(&self.db_home, &new_path);
    DBResponse::OpenContext((Box::new(directory), directory_id.to_string()))
  }

  fn compact(&self, _directory_id: &str) -> DBResponse<(Box<dyn ContextProcess>, String)> {
    DBResponse::Invalid("Compact is not applicable to directories".to_string())
  }
}
