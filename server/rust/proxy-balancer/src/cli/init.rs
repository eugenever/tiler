use std::process::exit;

use crate::config::load_db_config;
use crate::db::{connection_pg_config_db, create_pg_config_db, init_db, init_pg_db};
use crate::environment::{init_dirs, setup_envs};

use sqlx::Connection;

pub async fn command_init(cwd: String) {
    let _ = setup_envs();
    let db_config = load_db_config();

    // initialize application directories
    if let Err(err) = init_dirs(&cwd).await {
        eprintln!(
            "Error initialize application directories: {}",
            err.to_string()
        );
        exit(1);
    }

    if let Err(err) = create_pg_config_db(&db_config).await {
        eprintln!("Error create PostgreSQL DataBase: {}", err.to_string());
        exit(1);
    }

    let conn = match connection_pg_config_db(&db_config).await {
        Err(err) => {
            eprintln!("{err}");
            exit(1);
        }
        Ok(c) => c,
    };

    let conn = match init_pg_db(&cwd, conn).await {
        Err(err) => {
            eprintln!("Error initialize PostgreSQL DataBase: {}", err.to_string());
            exit(1);
        }
        Ok(conn) => conn,
    };
    conn.close()
        .await
        .expect("Error close PostgreSQL connection");

    // check exist DB tiler.db
    if let Err(err) = init_db(&cwd).await {
        eprintln!("Error initialize DataBase 'tiler.db': {}", err.to_string());
        exit(1);
    }
}
