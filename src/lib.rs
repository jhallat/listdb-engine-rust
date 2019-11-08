pub struct DBEngine {
    path: String,
}

impl DBEngine {
    pub fn new(path: &str) -> DBEngine {
        DBEngine {
            path: path.to_string(),
        }
    }

    pub fn process(&self, command_line: &str) {
        println!("DBEngine({}) {}", self.path, command_line);
    }
}
