use strum_macros::{AsRefStr, Display, EnumString, IntoStaticStr};

#[derive(Debug, Clone, Copy, PartialEq, Display, EnumString, IntoStaticStr, AsRefStr)]
pub enum EndPoints {
    // API
    #[strum(serialize = "/api/tile")]
    Tile,
    #[strum(serialize = "/api/pyramid")]
    Pyramid,
    #[strum(serialize = "/api/datasources")]
    DataSources,
    #[strum(serialize = "/api/datasources/load_files")]
    DataSourcesLoadFiles,
    #[strum(serialize = "/api/datasources/reload_files")]
    DataSourcesReloadFiles,
    #[strum(serialize = "/api/health")]
    Health,

    // Jobs
    #[strum(serialize = "/api/jobs")]
    Jobs,

    // Debug
    #[strum(serialize = "/debug")]
    Debug,

    // Maintenance
    #[strum(serialize = "/maintenance/add_workers")]
    AddWorkers,
    #[strum(serialize = "/maintenance/reload_workers")]
    ReloadWorkers,
    #[strum(serialize = "/maintenance/terminate_workers")]
    TerminateWorkers,
    #[strum(serialize = "/maintenance/info_workers")]
    InfoWorkers,

    #[strum(serialize = "/maintenance/increase_limit_cr")]
    IncreaseLimitConcurrentRequests,
    #[strum(serialize = "/maintenance/decrease_limit_cr")]
    DecreaseLimitConcurrentRequests,

    //Static assets
    #[strum(serialize = "/static")]
    Static,
}

#[derive(Debug, Clone, Copy, PartialEq, Display, EnumString, IntoStaticStr, AsRefStr)]
pub enum Extension {
    #[strum(serialize = "png")]
    Png,
    #[strum(serialize = "pbf")]
    Pbf,
    #[strum(serialize = "mvt")]
    Mvt,
}

#[derive(Debug, Clone, Copy, PartialEq, Display, EnumString, IntoStaticStr, AsRefStr)]
pub enum ContentType {
    #[strum(serialize = "image/png")]
    Png,
    #[strum(serialize = "application/vnd.mapbox-vector-tile")]
    MvtPbf,
    #[strum(serialize = "application/json")]
    ApplicationJson,
    #[strum(serialize = "")]
    Empty,
}
