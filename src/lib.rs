pub enum DBResponse {
    Ok(String),
    Invalid(String),
    Error(String),
    Unknown,
}

pub struct DBEngine {
    path: String,
}

impl DBEngine {
    pub fn new(path: &str) -> DBEngine {
        DBEngine {
            path: path.to_string(),
        }
    }

    pub fn process(&self, command_line: &str) -> DBResponse {
        let tokens: Vec<&str> = command_line.split(' ').collect();
        if tokens.len() == 0 {
            return DBResponse::Invalid("Empty command string".to_string());
        }
        //let result = format!("DBEngine({}) {}", self.path, command_line);
        DBResponse::Unknown
    }
}
