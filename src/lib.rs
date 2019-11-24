extern crate chrono;
extern crate uuid;

#[macro_use]
extern crate log;
extern crate env_logger;

use dbprocess::ContextProcess;
use dbprocess::DBResponse;
use dbprocess::RootContext;
use directories::Directories;
use std::collections::VecDeque;
use topics::Topics;

mod directories;
mod topics;

pub mod dbprocess {

    use crate::directories::Directories;
    use crate::topics::Topics;

    pub enum DBResponse<T> {
        ROk(String),
        Data(Vec<(String, String)>),
        Exit,
        Invalid(String),
        Error(String),
        OpenContext(T),
        CloseContext,
        Unknown,
    }

    enum Target {
        Topic,
        Directory,
        None,
    }

    pub trait ContextProcess {
        fn process(&mut self, command_line: &str) -> DBResponse<(Box<dyn ContextProcess>, String)>;
        fn id(&self) -> String;
    }

    pub struct RootContext {
        pub topics: Topics,
        pub directories: Directories,
    }

    struct Request {
        command: String,
        target: Target,
        arguments: Option<String>,
    }

    fn parse_request(request: &str) -> Result<Request, &'static str> {
        if request.len() == 0 {
            return Err("nothing to parse");
        }
        let tokens: Vec<&str> = request.split(' ').collect();
        let command = tokens.get(0).unwrap();
        if tokens.len() == 1 {
            return Ok(Request {
                command: command.to_string(),
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
                    command: command.to_string(),
                    target: Target::Topic,
                    arguments: arguments,
                })
            }
            "DIRECTORY" => {
                return Ok(Request {
                    command: command.to_string(),
                    target: Target::Directory,
                    arguments: arguments,
                })
            }
            _ => {
                return Ok(Request {
                    command: command.to_string(),
                    target: Target::None,
                    arguments: Some(tokens[1..].join(" ")),
                })
            }
        }
    }

    impl RootContext {
        fn list(&self, request: &Request) -> DBResponse<(Box<dyn ContextProcess>, String)> {
            match &request.target {
                Target::Topic => {
                    let list = self.topics.list();
                    DBResponse::Data(list)
                }
                Target::Directory => {
                    let list = self.directories.list();
                    DBResponse::Data(list)
                }
                _ => DBResponse::Invalid(
                    "Valid type required. (expected \"TOPIC\" or \"DIRECTORY\")".to_string(),
                ),
            }
        }

        fn status(&self) -> DBResponse<(Box<dyn ContextProcess>, String)> {
            let mut items: Vec<(String, String)> = Vec::new();
            let path = self.topics.db_home.clone();
            let property = format!("database.home: {}", path);
            items.push(("".to_string(), property.to_string()));
            DBResponse::Data(items)
        }

        fn create(&self, request: &Request) -> DBResponse<(Box<dyn ContextProcess>, String)> {
            match &request.arguments {
                Some(arguments) => match &request.target {
                    Target::Topic => match self.topics.create(&arguments) {
                        Ok(message) => DBResponse::ROk(message.to_string()),
                        Err(message) => DBResponse::Error(message.to_string()),
                    },
                    Target::Directory => DBResponse::Error("Not implmented".to_string()),
                    _ => DBResponse::Invalid(
                        "Valid type required. (expected \"TOPIC\" or \"DIRECTORY\")".to_string(),
                    ),
                },
                None => return DBResponse::Invalid("ID required.".to_string()),
            }
        }

        fn drop(&self, request: &Request) -> DBResponse<(Box<dyn ContextProcess>, String)> {
            match &request.arguments {
                Some(arguments) => match &request.target {
                    Target::Topic => match self.topics.drop(&arguments) {
                        Ok(message) => DBResponse::ROk(message.to_string()),
                        Err(message) => DBResponse::Error(message.to_string()),
                    },
                    Target::Directory => DBResponse::Error("Not implmented".to_string()),
                    _ => DBResponse::Invalid(
                        "Valid type required. (expected \"TOPIC\" or \"DIRECTORY\")".to_string(),
                    ),
                },
                None => return DBResponse::Invalid("ID required.".to_string()),
            }
        }

        fn open(&self, request: &Request) -> DBResponse<(Box<dyn ContextProcess>, String)> {
            match &request.arguments {
                Some(arguments) => match &request.target {
                    Target::Topic => self.topics.open(&arguments),
                    Target::Directory => DBResponse::Error("Not implmented".to_string()),
                    _ => DBResponse::Invalid(
                        "Not a valid type. (expected \"TOPIC\" or \"DIRECTORY\")".to_string(),
                    ),
                },
                None => return DBResponse::Invalid("ID required.".to_string()),
            }
        }

        fn compact(&self, request: &Request) -> DBResponse<(Box<dyn ContextProcess>, String)> {
            match &request.arguments {
                Some(arguments) => match &request.target {
                    Target::Topic => self.topics.compact(arguments),
                    _ => DBResponse::Invalid("Not a valid type. (expected \"TOPIC\")".to_string()),
                },
                None => return DBResponse::Invalid("ID required.".to_string()),
            }
        }
    }

    impl ContextProcess for RootContext {
        fn id(&self) -> String {
            "".to_string()
        }

        fn process(&mut self, request_text: &str) -> DBResponse<(Box<dyn ContextProcess>, String)> {
            //let tokens: Vec<&str> = command_line.split(' ').collect();
            //if tokens.len() == 0 {
            //    return DBResponse::Invalid("Empty command string".to_string());
            //}
            //let command: &str = &tokens[0].to_string().trim().to_uppercase();

            match parse_request(request_text) {
                Ok(parsed) => match parsed.command.as_str() {
                    "LIST" => self.list(&parsed),
                    "STATUS" => self.status(),
                    "CREATE" => self.create(&parsed),
                    "OPEN" => self.open(&parsed),
                    "COMPACT" => self.compact(&parsed),
                    "DROP" => self.drop(&parsed),
                    "EXIT" => DBResponse::Exit,
                    _ => DBResponse::Unknown,
                },
                Err(message) => return DBResponse::Error(message.to_string()),
            }
        }
    }
}

pub struct DBEngine {
    context_stack: VecDeque<Box<dyn ContextProcess>>,
}

impl DBEngine {
    pub fn new(path: &str) -> DBEngine {
        let root_context = RootContext {
            topics: Topics {
                db_home: path.to_string(),
            },
            directories: Directories {
                db_home: path.to_string(),
            },
        };
        let mut db_engine = DBEngine {
            context_stack: VecDeque::new(),
        };
        db_engine.context_stack.push_front(Box::new(root_context));
        db_engine
    }

    pub fn request(&mut self, db_request: &str) -> DBResponse<String> {
        //TODO Replace following line with debug logging?
        debug!(
            "context stack size: {} for '{}'",
            self.context_stack.len(),
            db_request
        );
        let mut context = self.context_stack.pop_front().unwrap();
        let result = context.process(db_request);
        self.context_stack.push_front(context);
        match result {
            DBResponse::ROk(message) => DBResponse::ROk(message),
            DBResponse::Data(data) => DBResponse::Data(data),
            DBResponse::Exit => DBResponse::Exit,
            DBResponse::Invalid(message) => DBResponse::Invalid(message),
            DBResponse::Error(message) => DBResponse::Error(message),
            DBResponse::OpenContext((context, message)) => {
                self.context_stack.push_front(context);
                DBResponse::OpenContext(message)
            }
            DBResponse::CloseContext => {
                self.context_stack.pop_front();
                let new_context = self.context_stack.front();
                let message = new_context.unwrap().id();
                DBResponse::OpenContext(message)
            }
            DBResponse::Unknown => DBResponse::Unknown,
        }
    }
}
