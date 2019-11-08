extern crate chrono;
extern crate uuid;

use std::collections::VecDeque;
use topics::Topics;

mod topics;

pub enum DBResponse<T> {
    ROk(String),
    Data(Vec<String>),
    Exit,
    Invalid(String),
    Error(String),
    OpenContext(T),
    Unknown,
}

pub trait ContextProcess {
    fn process(&self, command_line: &str) -> DBResponse<(Box<dyn ContextProcess>, String)>;
}

pub struct DBEngine {
    context_stack: VecDeque<Box<dyn ContextProcess>>,
}

pub struct RootContext {
    topics: Topics,
}

impl RootContext {
    fn list(topics: &Topics, args: &[&str]) -> DBResponse<(Box<dyn ContextProcess>, String)> {
        if args.len() == 0 {
            return DBResponse::Invalid("List requires a type".to_string());
        }
        if args.len() > 1 {
            return DBResponse::Invalid("List only takes one parameter: <type>".to_string());
        }
        let target: &str = &args[0].to_string().trim().to_uppercase();
        match target {
            "TOPIC" | "TOPICS" => {
                let list = topics.list();
                DBResponse::Data(list)
            }
            _ => DBResponse::Invalid("Not a valid type. (expected \"TOPIC\")".to_string()),
        }
    }

    fn status(&self) -> DBResponse<(Box<dyn ContextProcess>, String)> {
        let mut items: Vec<String> = Vec::new();
        let path = self.topics.db_home.clone();
        let property = format!("database.home: {}", path);
        items.push(property.to_string());
        DBResponse::Data(items)
    }

    fn create(&self, args: &[&str]) -> DBResponse<(Box<dyn ContextProcess>, String)> {
        if args.len() != 2 {
            return DBResponse::Invalid("Create takes two parameters: <type> <id>".to_string());
        }
        let target: &str = &args[0].to_string().trim().to_uppercase();
        match target {
            "TOPIC" => match self.topics.create(args[1]) {
                Ok(message) => DBResponse::ROk(message.to_string()),
                Err(message) => DBResponse::Error(message.to_string()),
            },
            _ => DBResponse::Invalid("Not a valid type. (expected \"TOPIC\")".to_string()),
        }
    }
}

impl ContextProcess for RootContext {
    fn process(&self, command_line: &str) -> DBResponse<(Box<dyn ContextProcess>, String)> {
        let tokens: Vec<&str> = command_line.split(' ').collect();
        if tokens.len() == 0 {
            return DBResponse::Invalid("Empty command string".to_string());
        }
        let command: &str = &tokens[0].to_string().trim().to_uppercase();
        match command {
            "LIST" => RootContext::list(&self.topics, &tokens[1..]),
            "STATUS" => self.status(),
            "CREATE" => self.create(&tokens[1..]),
            "EXIT" => DBResponse::Exit,
            _ => DBResponse::Unknown,
        }
    }
}

impl DBEngine {
    pub fn new(path: &str) -> DBEngine {
        let root_context = RootContext {
            topics: Topics {
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
        let context = self.context_stack.front().unwrap();
        match context.process(db_request) {
            DBResponse::ROk(message) => DBResponse::ROk(message),
            DBResponse::Data(data) => DBResponse::Data(data),
            DBResponse::Exit => DBResponse::Exit,
            DBResponse::Invalid(message) => DBResponse::Invalid(message),
            DBResponse::Error(message) => DBResponse::Error(message),
            DBResponse::OpenContext((context, message)) => {
                self.context_stack.push_front(context);
                DBResponse::OpenContext(message)
            }
            DBResponse::Unknown => DBResponse::Unknown,
        }
    }
}
