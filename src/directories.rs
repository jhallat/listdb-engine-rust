use std::fs;

/// Manages directories in the database
pub struct Directories {
  /// Location of the database
  pub db_home: String,
}

impl Directories {
  pub fn list(&self) -> Vec<(String, String)> {
    let mut items: Vec<(String, String)> = Vec::new();
    let files = fs::read_dir(self.db_home.clone()).unwrap();
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
}
