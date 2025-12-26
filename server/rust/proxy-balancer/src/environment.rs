use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::anyhow;
use dotenv::dotenv;
use std::process::exit;

pub fn setup_envs() -> HashMap<&'static str, String> {
    match dotenv() {
        Ok(_) => {}
        Err(err) => {
            eprintln!("Error load '.env' file: {}", err);
            exit(1);
        }
    }

    let mut vars = HashMap::new();

    if cfg!(unix) {
        let gdal_home = match std::env::var("GDAL_HOME") {
            Ok(gh) => gh,
            Err(err) => {
                eprintln!("'GDAL_HOME' environment variable is not defined: {err}");
                exit(1);
            }
        };

        let python_path = match std::env::var("PYTHONPATH") {
            Ok(pp) => pp,
            Err(err) => {
                eprintln!("'PYTHONPATH' environment variable is not defined: {err}");
                exit(1);
            }
        };

        let path = std::env::var("PATH").unwrap_or(String::default());
        let mut paths = std::env::split_paths(&path).collect::<Vec<PathBuf>>();
        paths.insert(0, std::path::PathBuf::from(&gdal_home));
        paths.insert(0, std::path::PathBuf::from(&gdal_home).join("bin"));
        paths.insert(0, std::path::PathBuf::from(&python_path));
        paths.insert(0, std::path::PathBuf::from(&python_path).join("bin"));
        let new_path = std::env::join_paths(paths).unwrap();
        std::env::set_var("PATH", &new_path);

        let ld_lib_path = std::env::var("LD_LIBRARY_PATH").unwrap_or(String::default());
        let mut ld_libs = std::env::split_paths(&ld_lib_path).collect::<Vec<PathBuf>>();
        ld_libs.push(std::path::PathBuf::from(&gdal_home).join("lib"));
        ld_libs.insert(0, std::path::PathBuf::from(&python_path).join("lib"));
        let new_ld_libs = std::env::join_paths(&ld_libs).unwrap();
        std::env::set_var("LD_LIBRARY_PATH", &new_ld_libs);

        let pr_lib = std::path::PathBuf::from(&gdal_home)
            .join("share")
            .join("proj");
        std::env::set_var("PROJ_LIB", &pr_lib);

        vars.insert("GDAL_HOME", gdal_home);
        vars.insert("PATH", new_path.into_string().unwrap());
        vars.insert("LD_LIBRARY_PATH", new_ld_libs.into_string().unwrap());
        vars.insert("PROJ_LIB", pr_lib.into_os_string().into_string().unwrap());
    }

    if cfg!(windows) {
        let gdal_home = match std::env::var("GDAL_HOME") {
            Ok(gh) => gh,
            Err(err) => {
                eprintln!("'GDAL_HOME' environment variable is not defined: {err}");
                exit(1);
            }
        };
        let pr_lib = match std::env::var("PROJ_LIB") {
            Ok(pl) => pl,
            Err(err) => {
                eprintln!("'PROJ_LIB' environment variable is not defined: {err}");
                exit(1);
            }
        };
        let python_path = match std::env::var("PYTHONPATH") {
            Ok(pp) => pp,
            Err(err) => {
                eprintln!("'PYTHONPATH' environment variable is not defined: {err}");
                exit(1);
            }
        };

        let path = match std::env::var("Path") {
            Ok(p) => p,
            Err(err) => {
                eprintln!("'Path' environment variable is not defined: {err}");
                exit(1);
            }
        };
        let mut paths = std::env::split_paths(&path).collect::<Vec<PathBuf>>();
        paths.insert(
            0,
            std::path::PathBuf::from(&gdal_home)
                .join("bin")
                .join("gdal")
                .join("apps"),
        );
        paths.insert(0, std::path::PathBuf::from(&gdal_home).join("bin"));
        paths.push(std::path::PathBuf::from(&python_path));
        paths.push(std::path::PathBuf::from(&python_path).join("Scripts"));
        let new_path = std::env::join_paths(paths).unwrap();
        std::env::set_var("Path", &new_path);

        vars.insert("GDAL_HOME", gdal_home);
        vars.insert("Path", new_path.into_string().unwrap());
        vars.insert("PROJ_LIB", pr_lib);
        match std::env::var("PROJ_DEBUG") {
            Ok(pd) => vars.insert("PROJ_DEBUG", pd),
            _ => None,
        };
    }

    vars
}

/*
    CWD = Current Working Directory
    CWD/data
    CWD/data/mosaics

    CWD/logs

    CWD/datasources
    CWD/datasources/vector
    CWD/datasources/raster
*/
pub async fn init_dirs(cwd: &str) -> Result<(), anyhow::Error> {
    let data_dir: PathBuf = [cwd, "data"].iter().collect();
    let mosaics_dir: PathBuf = [cwd, "data", "mosaics"].iter().collect();
    if !tokio::fs::try_exists(&data_dir).await? {
        tokio::fs::create_dir(data_dir)
            .await
            .map_err(|err| anyhow!(err))?;
        tokio::fs::create_dir(mosaics_dir)
            .await
            .map_err(|err| anyhow!(err))?;
    } else {
        if !tokio::fs::try_exists(&mosaics_dir).await? {
            tokio::fs::create_dir(mosaics_dir)
                .await
                .map_err(|err| anyhow!(err))?;
        }
    }

    let logs_dir: PathBuf = [cwd, "logs"].iter().collect();
    if !tokio::fs::try_exists(&logs_dir).await? {
        tokio::fs::create_dir(logs_dir)
            .await
            .map_err(|err| anyhow!(err))?;
    }

    let ds_dir: PathBuf = [cwd, "datasources"].iter().collect();
    let ds_vector_dir: PathBuf = [cwd, "datasources", "vector"].iter().collect();
    let ds_raster_dir: PathBuf = [cwd, "datasources", "raster"].iter().collect();
    if !tokio::fs::try_exists(&ds_dir).await? {
        tokio::fs::create_dir(ds_dir)
            .await
            .map_err(|err| anyhow!(err))?;
        tokio::fs::create_dir(ds_vector_dir)
            .await
            .map_err(|err| anyhow!(err))?;
        tokio::fs::create_dir(ds_raster_dir)
            .await
            .map_err(|err| anyhow!(err))?;
    } else {
        if !tokio::fs::try_exists(&ds_vector_dir).await? {
            tokio::fs::create_dir(ds_vector_dir)
                .await
                .map_err(|err| anyhow!(err))?;
        }
        if !tokio::fs::try_exists(&ds_raster_dir).await? {
            tokio::fs::create_dir(ds_raster_dir)
                .await
                .map_err(|err| anyhow!(err))?;
        }
    }

    let tiles_dir: PathBuf = [cwd, "tiles"].iter().collect();
    if !tokio::fs::try_exists(&tiles_dir).await? {
        tokio::fs::create_dir(tiles_dir)
            .await
            .map_err(|err| anyhow!(err))?;
    }

    Ok(())
}

pub fn get_cwd() -> Result<String, anyhow::Error> {
    let cwd = std::env::current_dir()?;
    Ok(cwd.to_string_lossy().to_string())
}
