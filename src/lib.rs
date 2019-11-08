extern crate chrono;
extern crate uuid;

use topics::Topics;

mod topics;

pub enum DBResponse {
    ROk(String),
    Data(Vec<String>),
    Exit,
    Invalid(String),
    Error(String),
    Unknown,
}

pub struct DBEngine {
    topics: Topics,
}

impl DBEngine {
    pub fn new(path: &str) -> DBEngine {
        DBEngine {
            topics: Topics {
                db_home: path.to_string(),
            },
        }
    }

    fn list(topics: &Topics, args: &[&str]) -> DBResponse {
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

    fn status(&self) -> DBResponse {
        let mut items: Vec<String> = Vec::new();
        let path = self.topics.db_home.clone();
        let property = format!("database.home: {}", path);
        items.push(property.to_string());
        DBResponse::Data(items)
    }

    fn create(&self, args: &[&str]) -> DBResponse {
        if args.len() != 2 {
            return DBResponse::Invalid("Create takes two parameters: <type> <id>".to_string());
        }
        let target: &str = &args[0].to_string().trim().to_uppercase();
        match target {
            "TOPIC" => {
                self.topics.create(args[1]);
                let response = format!("Topic {} created.", args[1]);
                DBResponse::ROk(response.to_string())
            }
            _ => DBResponse::Invalid("Not a valid type. (expected \"TOPIC\")".to_string()),
        }
    }

    pub fn process(&self, command_line: &str) -> DBResponse {
        let tokens: Vec<&str> = command_line.split(' ').collect();
        if tokens.len() == 0 {
            return DBResponse::Invalid("Empty command string".to_string());
        }
        let command: &str = &tokens[0].to_string().trim().to_uppercase();
        match command {
            "LIST" => DBEngine::list(&self.topics, &tokens[1..]),
            "STATUS" => self.status(),
            "CREATE" => self.create(&tokens[1..]),
            "EXIT" => DBResponse::Exit,
            _ => DBResponse::Unknown,
        }
    }
}
