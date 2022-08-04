use chrono::Utc;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, Publish, QoS};
use sqlx::postgres::PgPool;

async fn handle_mqtt_event(e: Publish, pool: &PgPool) -> anyhow::Result<()> {
    let topic = e.topic.split('/').last().expect("Cannot have empty topic");
    match topic {
        "power" => insert_into_db(&e.payload, pool).await?,
        s => println!(
            "[{}] {}: {}",
            Utc::now(),
            s,
            String::from_utf8_lossy(&e.payload)
        ),
    }
    Ok(())
}

async fn insert_into_db(payload: &[u8], pool: &PgPool) -> anyhow::Result<()> {
    let value: f64 = String::from_utf8_lossy(payload).parse()?;
    sqlx::query!("insert into power (value) values ($1)", value)
        .execute(pool)
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use std::env::var;
    dotenvy::dotenv().ok();
    let pool = PgPool::connect(&var("DATABASE_URL").expect("DATABASE_URL must be set")).await?;
    sqlx::migrate!().run(&pool).await?;

    let mut mqttoptions = MqttOptions::new(
        "pg_logger",
        &var("MQTT_HOST").expect("MQTT_HOST must be set"),
        var("MQTT_PORT")
            .expect("MQTT_PORT must be set")
            .parse()
            .expect("MQTT_PORT must be a valid port number"),
    );
    mqttoptions.set_credentials(
        var("MQTT_USER").expect("MQTT_USER must be set"),
        var("MQTT_PASS").expect("MQTT_PASS must be set"),
    );
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    client.subscribe("#", QoS::AtLeastOnce).await?;

    loop {
        if let Event::Incoming(Packet::Publish(e)) = eventloop.poll().await? {
            // just block the whole thing. who cares
            handle_mqtt_event(e, &pool).await?;
        }
    }
}
