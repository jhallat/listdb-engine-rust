use std::fs;
use std::fs::File;
use std::path::Path;

/// Manages directories in the database
pub struct Directories {
  /// Location of the database
  pub db_home: String,
}

impl Directories {
  pub fn create(&self, directory_id: &str) -> Result<String, String> {
    if self.directory_exists(&directory_id) {
      let message = format!("The directory {} already exists.", directory_id);
      return Err(message);
    }
    match File::create(self.directory_path(directory_id)) {
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

  fn directory_path(&self, directory_id: &str) -> String {
    format!("{}\\{}", self.db_home, directory_id)
  }

  fn directory_exists(&self, directory_id: &str) -> bool {
    let directory_path = self.directory_path(directory_id);
    Path::new(&directory_path).exists()
  }
}
