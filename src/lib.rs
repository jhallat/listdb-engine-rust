extern crate chrono;
extern crate uuid;

#[macro_use]
extern crate log;
extern crate env_logger;

use dbprocess::ContextProcess;
use dbprocess::DBResponse;
use directories::DirectoryContext;
use std::collections::VecDeque;

mod directories;
mod topics;

pub mod dbprocess {
    pub enum DBResponse<T> {
        ROk(String),
        Data(Vec<(String, String)>),
        Exit,
        Invalid(String),
        Error(String),
        OpenContext(T),
        CloseContext,
        Created(String),
        Unknown(String),
    }

    pub trait ContextProcess {
        fn process(&mut self, command_line: &str) -> DBResponse<(Box<dyn ContextProcess>, String)>;
        fn id(&self) -> String;
    }

    pub trait ContextController {
        fn create(&self, id: &str) -> Result<String, String>;
        fn drop_item(&self, id: &str) -> Result<String, String>;
        fn list(&self) -> Vec<(String, String)>;
        fn open(&self, id: &str) -> DBResponse<(Box<dyn ContextProcess>, String)>;
        fn compact(&self, id: &str) -> DBResponse<(Box<dyn ContextProcess>, String)>;
    }
}

pub struct DBEngine {
    context_stack: VecDeque<Box<dyn ContextProcess>>,
}

impl DBEngine {
    pub fn new(path: &str) -> DBEngine {
        let root_context = DirectoryContext::new(&path, "\\");
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
            DBResponse::Created(message) => DBResponse::Created(message),
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
            DBResponse::Unknown(message) => DBResponse::Unknown(message),
        }
    }
}
