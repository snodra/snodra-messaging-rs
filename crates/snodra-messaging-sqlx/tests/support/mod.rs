//! Test databases: a PostgreSQL or MySQL container at the image versions the tests
//! pin (`postgres:17.7`, `mysql:8.4.7`), a fresh database per check, and raw SQL (literal
//! test values, every column read back as text) to look at the stored rows apart from the store.
#![allow(dead_code)]

use std::time::Duration;

use snodra_messaging_sqlx::{SqlxSettings, SqlxStore};
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{AssertSqlSafe, Row};
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::{mysql::Mysql, postgres::Postgres};

pub const POSTGRES_TAG: &str = "17.7";
pub const MYSQL_TAG: &str = "8.4.7";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Postgres,
    MySql,
}

/// A database server in a container, removed when dropped.
pub struct Server {
    pub kind: Kind,
    /// The server's URL without a database (test credentials of the container image).
    base: String,
    _pg: Option<ContainerAsync<Postgres>>,
    _my: Option<ContainerAsync<Mysql>>,
}

/// Retries `connect` until the forwarded port answers (Rancher Desktop's port forwarding lags
/// behind the server's ready line).
async fn eventually<T, F: AsyncFnMut() -> Result<T, sqlx::Error>>(mut connect: F) -> T {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        match connect().await {
            Ok(v) => return v,
            Err(e) if tokio::time::Instant::now() < deadline => {
                let _ = e;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(e) => panic!("the test database does not answer: {e}"),
        }
    }
}

impl Server {
    pub async fn postgres() -> Server {
        let container = Postgres::default()
            .with_tag(POSTGRES_TAG)
            .start()
            .await
            .expect("postgres container");
        let port = container
            .get_host_port_ipv4(5432.tcp())
            .await
            .expect("postgres port");
        let base = format!("postgres://postgres:postgres@127.0.0.1:{port}");
        let admin = format!("{base}/postgres");
        eventually(async || PgPool::connect(&admin).await.map(|_| ())).await;
        Server {
            kind: Kind::Postgres,
            base,
            _pg: Some(container),
            _my: None,
        }
    }

    pub async fn mysql() -> Server {
        let container = Mysql::default()
            .with_tag(MYSQL_TAG)
            .start()
            .await
            .expect("mysql container");
        let port = container
            .get_host_port_ipv4(3306.tcp())
            .await
            .expect("mysql port");
        let base = format!("mysql://root@127.0.0.1:{port}");
        let admin = format!("{base}/test");
        eventually(async || MySqlPool::connect(&admin).await.map(|_| ())).await;
        Server {
            kind: Kind::MySql,
            base,
            _pg: None,
            _my: Some(container),
        }
    }

    pub async fn of(kind: Kind) -> Server {
        match kind {
            Kind::Postgres => Server::postgres().await,
            Kind::MySql => Server::mysql().await,
        }
    }

    /// A fresh, empty database; its URL.
    pub async fn database(&self) -> String {
        let name = format!("k{}", uuid::Uuid::new_v4().simple());
        match self.kind {
            Kind::Postgres => {
                let admin = PgPool::connect(&format!("{}/postgres", self.base))
                    .await
                    .unwrap();
                sqlx::raw_sql(AssertSqlSafe(format!("CREATE DATABASE {name}")))
                    .execute(&admin)
                    .await
                    .unwrap();
                admin.close().await;
            }
            Kind::MySql => {
                let admin = MySqlPool::connect(&format!("{}/test", self.base))
                    .await
                    .unwrap();
                sqlx::raw_sql(AssertSqlSafe(format!("CREATE DATABASE {name}")))
                    .execute(&admin)
                    .await
                    .unwrap();
                admin.close().await;
            }
        }
        format!("{}/{name}", self.base)
    }

    /// A store on a fresh database (schema applied), and raw access to that database.
    pub async fn store(&self) -> (SqlxStore, Raw) {
        self.store_with(SqlxSettings::default()).await
    }

    pub async fn store_with(&self, settings: SqlxSettings) -> (SqlxStore, Raw) {
        let url = self.database().await;
        let (store, raw) = self.open(&url, settings).await;
        store.apply_schema().await.expect("schema");
        (store, raw)
    }

    /// A store and raw access on the database at `url` (nothing applied).
    pub async fn open(&self, url: &str, settings: SqlxSettings) -> (SqlxStore, Raw) {
        match self.kind {
            Kind::Postgres => {
                let pool = PgPoolOptions::new()
                    .max_connections(20)
                    .connect(url)
                    .await
                    .unwrap();
                (
                    SqlxStore::postgres(pool.clone()).with_settings(settings),
                    Raw::Pg(pool),
                )
            }
            Kind::MySql => {
                let pool = MySqlPoolOptions::new()
                    .max_connections(20)
                    .connect(url)
                    .await
                    .unwrap();
                (
                    SqlxStore::mysql(pool.clone()).with_settings(settings),
                    Raw::My(pool),
                )
            }
        }
    }
}

/// `url` with the credentials `user:password` instead of the container's.
pub fn as_user(url: &str, user: &str, password: &str) -> String {
    let (scheme, rest) = url.split_once("://").expect("a URL");
    let (_, host) = rest.split_once('@').expect("credentials in the URL");
    format!("{scheme}://{user}:{password}@{host}")
}

/// The database name at the end of `url`.
pub fn database_name(url: &str) -> &str {
    url.rsplit('/').next().expect("a database")
}

/// Raw SQL on a test database: literal values only, every selected column cast to text in the
/// statement (`{text}` is `TEXT` / `CHAR`).
#[derive(Clone)]
pub enum Raw {
    Pg(PgPool),
    My(MySqlPool),
}

impl Raw {
    pub fn kind(&self) -> Kind {
        match self {
            Raw::Pg(_) => Kind::Postgres,
            Raw::My(_) => Kind::MySql,
        }
    }

    fn render(&self, sql: &str) -> String {
        match self {
            Raw::Pg(_) => sql.replace("{text}", "TEXT").replace("{json}", "jsonb"),
            Raw::My(_) => sql.replace("{text}", "CHAR").replace("{json}", "JSON"),
        }
    }

    /// A timestamp column as `YYYY-MM-DDTHH:MM:SS.mmm`, the UTC instant it holds.
    pub fn ts(&self, column: &str) -> String {
        match self {
            Raw::Pg(_) => {
                format!("to_char({column} AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS')")
            }
            Raw::My(_) => format!("LEFT(DATE_FORMAT({column}, '%Y-%m-%dT%H:%i:%s.%f'), 23)"),
        }
    }

    /// A UTC timestamp literal as the column type takes it.
    pub fn at(&self, utc: &str) -> String {
        match self {
            Raw::Pg(_) => format!("TIMESTAMPTZ '{utc}+00'"),
            Raw::My(_) => format!("'{utc}'"),
        }
    }

    pub async fn exec(&self, sql: &str) -> u64 {
        let sql = self.render(sql);
        match self {
            Raw::Pg(p) => sqlx::raw_sql(AssertSqlSafe(sql))
                .execute(p)
                .await
                .unwrap()
                .rows_affected(),
            Raw::My(p) => sqlx::raw_sql(AssertSqlSafe(sql))
                .execute(p)
                .await
                .unwrap()
                .rows_affected(),
        }
    }

    /// Every row, every column as text (`None` for NULL).
    pub async fn rows(&self, sql: &str) -> Vec<Vec<Option<String>>> {
        let sql = self.render(sql);
        match self {
            Raw::Pg(p) => sqlx::query(AssertSqlSafe(sql))
                .fetch_all(p)
                .await
                .unwrap()
                .iter()
                .map(|r| {
                    (0..r.len())
                        .map(|i| r.try_get::<Option<String>, _>(i).unwrap())
                        .collect()
                })
                .collect(),
            Raw::My(p) => sqlx::query(AssertSqlSafe(sql))
                .fetch_all(p)
                .await
                .unwrap()
                .iter()
                .map(|r| {
                    (0..r.len())
                        .map(|i| r.try_get::<Option<String>, _>(i).unwrap())
                        .collect()
                })
                .collect(),
        }
    }

    /// The one row of `sql`, as text.
    pub async fn row(&self, sql: &str) -> Vec<Option<String>> {
        let mut rows = self.rows(sql).await;
        assert_eq!(rows.len(), 1, "one row of {sql}");
        rows.remove(0)
    }

    pub async fn count(&self, table: &str) -> i64 {
        self.row(&format!("SELECT CAST(COUNT(*) AS {{text}}) FROM {table}"))
            .await[0]
            .as_deref()
            .unwrap()
            .parse()
            .unwrap()
    }
}

/// `Some(text)` for each value: a row's expected text.
pub fn texts(values: &[Option<&str>]) -> Vec<Option<String>> {
    values.iter().map(|v| v.map(str::to_string)).collect()
}
