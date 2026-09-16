use super::*;

fn database_url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL must point to the test database")
}

async fn prepared_name(connection: &mut PgConnection, query: &str) -> String {
    sqlx::query_scalar("SELECT name FROM pg_prepared_statements WHERE statement = $1")
        .bind(query)
        .persistent(false)
        .fetch_one(connection)
        .await
        .expect("read prepared statement name")
}

async fn exercise_cache(options: PgConnectOptions, expect_reuse: bool, capacity: usize) {
    let mut connection = options.connect().await.expect("connect cache test");
    let first = "SELECT $1::int4 AS cache_probe_first";
    sqlx::query(first)
        .bind(1)
        .execute(&mut connection)
        .await
        .unwrap();
    let original = prepared_name(&mut connection, first).await;
    for index in 0..128 {
        sqlx::query(&format!("SELECT $1::int4 AS cache_probe_{index}"))
            .bind(index)
            .execute(&mut connection)
            .await
            .unwrap();
    }
    sqlx::query(first)
        .bind(2)
        .execute(&mut connection)
        .await
        .unwrap();
    let reused = prepared_name(&mut connection, first).await == original;
    assert_eq!(reused, expect_reuse);
    assert_eq!(connection.cached_statements_size(), capacity.min(129));
    connection.close().await.unwrap();
}

#[tokio::test]
async fn direct_cache_reuses_statements_beyond_the_previous_capacity() {
    let options = pg_connect_options(&database_url(), Some(false), ConnectionMode::Direct).unwrap();
    exercise_cache(options, true, 256).await;
}

#[tokio::test]
async fn explicit_url_cache_capacity_is_preserved() {
    let mut url = url::Url::parse(&database_url()).unwrap();
    url.query_pairs_mut()
        .append_pair("statement-cache-capacity", "8");
    let options = pg_connect_options(url.as_str(), Some(false), ConnectionMode::Direct).unwrap();
    exercise_cache(options, false, 8).await;
}

#[tokio::test]
async fn explicit_zero_and_transaction_mode_do_not_cache_statements() {
    for (mode, capacity) in [
        (ConnectionMode::Direct, "0"),
        (ConnectionMode::Transaction, "256"),
    ] {
        let mut url = url::Url::parse(&database_url()).unwrap();
        url.query_pairs_mut()
            .append_pair("statement-cache-capacity", capacity);
        let options = pg_connect_options(url.as_str(), Some(false), mode).unwrap();
        let mut connection = options.connect().await.unwrap();
        for _ in 0..2 {
            sqlx::query("SELECT $1::int4 AS uncached_probe")
                .bind(1)
                .execute(&mut connection)
                .await
                .unwrap();
            assert_eq!(connection.cached_statements_size(), 0);
        }
        connection.close().await.unwrap();
    }
}

#[tokio::test]
async fn transaction_session_cache_keeps_its_previous_capacity() {
    let options = pg_connect_options(
        &database_url(),
        Some(false),
        ConnectionMode::TransactionSession,
    )
    .unwrap();
    exercise_cache(options, false, 100).await;
}

#[tokio::test]
async fn idle_check_skips_recent_connections_and_rejects_a_closed_backend() {
    let options = pg_connect_options(&database_url(), Some(false), ConnectionMode::Direct).unwrap();
    let mut connection = options.connect().await.unwrap();
    assert!(ping_after_idle(&mut connection, Duration::from_secs(60))
        .await
        .unwrap());
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    let mut control = options.connect().await.unwrap();
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1, 1000)")
        .bind(pid)
        .fetch_one(&mut control)
        .await
        .unwrap();
    assert!(terminated);
    assert!(ping_after_idle(&mut connection, Duration::from_secs(59))
        .await
        .unwrap());
    assert!(ping_after_idle(&mut connection, Duration::from_secs(60))
        .await
        .is_err());
    control.close().await.unwrap();
}

#[tokio::test]
async fn raw_constructor_keeps_pool_size_and_skips_schema_initialization() {
    let broker = PostgresBroker::connect(&database_url()).await.unwrap();
    assert_eq!(broker.pool.options().get_max_connections(), 10);
    assert_eq!(
        broker.pool.options().get_max_lifetime(),
        Some(Duration::from_secs(7200))
    );
    assert!(!broker.pool.options().get_test_before_acquire());
    assert!(broker.schema_initialized.get().is_none());
    broker.pool.close().await;
}

#[test]
fn transaction_pool_limits_and_health_checks_keep_their_previous_settings() {
    let mut config = PostgresConfig::from_url("postgresql://localhost/test");
    config.pgbouncer_transaction_mode = true;
    config.session_database_url = Some("postgresql://localhost/session".to_owned());
    config.pool_size = 7;
    config.max_overflow = 3;
    config.pool_timeout = 9;
    config.pool_recycle = 123;
    for pre_ping in [false, true] {
        config.pool_pre_ping = pre_ping;
        for (pool, max_connections) in [
            (pg_pool_options(&config), 10),
            (pg_session_pool_options(&config), 4),
        ] {
            assert_eq!(pool.get_max_connections(), max_connections);
            assert_eq!(pool.get_acquire_timeout(), Duration::from_secs(9));
            assert_eq!(pool.get_idle_timeout(), Some(Duration::from_secs(123)));
            assert_eq!(pool.get_max_lifetime(), Some(Duration::from_secs(1800)));
            assert_eq!(pool.get_test_before_acquire(), pre_ping);
        }
    }
}
