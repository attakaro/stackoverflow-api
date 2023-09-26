#[macro_use]
extern crate rocket;

extern crate log;

mod cors;
mod handlers;
mod models;
mod persistance;

use cors::*;
use handlers::*;

use persistance::{ 
    questions_dao::{ 
        QuestionsDaoImpl, 
        QuestionsDao
    }, 
    answers_dao::{
        AnswersDaoImpl, 
        AnswersDao
    }
};

use pretty_env_logger::env_logger;
use dotenvy;

use sqlx::postgres::PgPoolOptions;

#[launch]
async fn rocket() -> _ {

    env_logger::init();
    dotenvy::dotenv().unwrap();

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&std::env::var("DATABASE_URL").expect("DATABASE_URL must be set."))
        .await
        .expect("Failed to create Postgres connection pool!");

    let questions_dao =  QuestionsDaoImpl::new(pool.clone()); 
    let answers_dao = AnswersDaoImpl::new(pool); 

    rocket::build()
        .mount(
            "/",
            routes![
                create_question,
                read_questions,
                delete_question,
                create_answer,
                read_answers,
                delete_answer
            ],
        )
        .attach(CORS)
        .manage(Box::new(questions_dao) as Box<dyn QuestionsDao + Send + Sync>) 
        .manage(Box::new(answers_dao) as Box<dyn AnswersDao + Send + Sync> ) 
}