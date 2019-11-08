pub enum DBResponse {
    OK(String),
    Invalid(String),
    Error(String),
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
        let result = format!("DBEngine({}) {}", self.path, command_line);
        DBResponse::OK(result)
    }
}
