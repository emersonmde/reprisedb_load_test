use rand::{Rng, SeedableRng};
use reprisedb::models::value;
use reprisedb::reprisedb::Database;
use reprisedb::reprisedb::DatabaseConfigBuilder;
use std::fs::{self, File};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const METRIC_INTERVAL: Duration = Duration::from_millis(100);

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, value_parser = parse_duration)]
    duration: Duration,

    #[arg(short, long)]
    num_tasks: usize,

    #[arg(short, long)]
    keyspace: u64,
}

fn parse_duration(arg: &str) -> Result<std::time::Duration, std::num::ParseIntError> {
    let seconds = arg.parse()?;
    Ok(std::time::Duration::from_secs(seconds))
}

#[tokio::main]
async fn main() {
    // console_subscriber::init();

    let args = Args::parse();
    println!("Starting RepriseDB load test");
    println!("Number of tasks: {}", args.num_tasks);
    println!("Duration: {} seconds", args.duration.as_secs());
    println!("Keyspace: {}", args.keyspace);
    println!("==========");

    let config = DatabaseConfigBuilder::new()
        .sstable_dir("/tmp/reprisedb_load_test".to_string())
        // .memtable_size_target(4 * 1024 * 1024)
        .memtable_size_target(1 * 1024 * 1024)
        .compaction_interval(Duration::from_millis(800))
        .build();
    let db = Database::new(config).await.unwrap();
    let start = Instant::now();
    let mut handles = Vec::new();

    let read_ops = Arc::new(AtomicUsize::new(0));
    let read_latency = Arc::new(AtomicUsize::new(0));
    let read_errors = Arc::new(AtomicUsize::new(0));

    let write_ops = Arc::new(AtomicUsize::new(0));
    let write_latency = Arc::new(AtomicUsize::new(0));
    let write_errors = Arc::new(AtomicUsize::new(0));

    let mut metrics_vec = Vec::new();
    metrics_vec.push((0.0, 0, 0.0, 0, 0, 0.0, 0, 0, 0, 0));
    println!("t: 0.00000000 | Read Ops: 0 | Read/s: 0 | Avg Read Lat: 0 | Write Ops: 0 | Write/s: 0 | Avg Write Lat: 0 | Read Err: 0 | Write Err: 0 | MemTable Size: 0");
    let metrics = Arc::new(Mutex::new(metrics_vec));

    for _ in 0..args.num_tasks {
        let mut db_clone = db.clone();
        let read_ops_clone = Arc::clone(&read_ops);
        let read_latency_clone = Arc::clone(&read_latency);
        let read_errors_clone = Arc::clone(&read_errors);

        let write_ops_clone = Arc::clone(&write_ops);
        let write_latency_clone = Arc::clone(&write_latency);
        let write_errors_clone = Arc::clone(&write_errors);

        let handle = tokio::spawn(async move {
            let start = start; // clone start time for this thread
            let mut rng = rand::rngs::StdRng::from_entropy();
            while Instant::now().duration_since(start) < args.duration {
                // Generate a random key within the keyspace
                let rand = rng.gen_range(0..args.keyspace).to_string();
                let key = format!("key{}", rand);
                // Use the elapsed time as the key, every put will be sequential and no reads
                // would succeed
                // let key = start.elapsed().as_micros().to_string();
                // let value = value::Kind::Int(rng.gen());
                let value_string = format!("value{}", rand);
                let value = value::Kind::Str(value_string);

                // 50% chance to perform a read operation
                if rng.gen_bool(0.1) {
                    let start_time = Instant::now();
                    match db_clone.get(&key).await {
                        Ok(_) => {
                            let elapsed_time = start_time.elapsed().as_micros() as usize;
                            read_latency_clone.fetch_add(elapsed_time, Ordering::SeqCst);
                            read_ops_clone.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(e) => {
                            eprintln!("Error during read operation: {}", e);
                            read_errors_clone.fetch_add(1, Ordering::SeqCst);
                        }
                    };
                } else {
                    let start_time = Instant::now();
                    match db_clone.put(key, value).await {
                        Ok(_) => {
                            let elapsed_time = start_time.elapsed().as_micros() as usize;
                            write_latency_clone.fetch_add(elapsed_time, Ordering::SeqCst);
                            write_ops_clone.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(e) => {
                            eprintln!("Error during write operation: {}", e);
                            write_errors_clone.fetch_add(1, Ordering::SeqCst);
                        }
                    };
                }
            }
        });
        handles.push(handle);
    }

    let reporting_read_ops = Arc::clone(&read_ops);
    let reporting_read_latency = Arc::clone(&read_latency);
    let reporting_read_errors = Arc::clone(&read_errors);

    let reporting_write_ops = Arc::clone(&write_ops);
    let reporting_write_latency = Arc::clone(&write_latency);
    let reporting_write_errors = Arc::clone(&write_errors);

    let reporting_metrics = Arc::clone(&metrics);
    let reporting_task_future = async {
        let start = Instant::now();
        while Instant::now().duration_since(start) < args.duration {
            tokio::time::sleep(METRIC_INTERVAL).await;
            let elapsed_time = start.elapsed().as_secs_f64();
            let read_ops = reporting_read_ops.load(Ordering::SeqCst);
            let read_latency = reporting_read_latency.load(Ordering::SeqCst);
            let read_errors = reporting_read_errors.load(Ordering::SeqCst);
            let avg_read_latency = if read_ops > 0 {
                read_latency / read_ops
            } else {
                0
            };

            let write_ops = reporting_write_ops.load(Ordering::SeqCst);
            let write_latency = reporting_write_latency.load(Ordering::SeqCst);
            let write_errors = reporting_write_errors.load(Ordering::SeqCst);
            let avg_write_latency = if write_ops > 0 {
                write_latency / write_ops
            } else {
                0
            };

            let memtable_size = db.memtable.read().await.size();

            let read_ops_per_sec = (read_ops as f64) / elapsed_time;
            let write_ops_per_sec = (write_ops as f64) / elapsed_time;

            println!(
                "t: {:.8} | Read ops: {} | Read/s: {:.0} | Avg Read Lat: {} | Write Ops: {} | Write/s {:.0} | Avg Write Lat: {} | Read Err: {} | Write Err: {} | MemTable Size: {}",
                elapsed_time,
                read_ops,
                read_ops_per_sec,
                avg_read_latency,
                write_ops,
                write_ops_per_sec,
                avg_write_latency,
                read_errors,
                write_errors,
                memtable_size
            );

            let mut metrics = reporting_metrics.lock().await;
            metrics.push((
                elapsed_time,
                read_ops,
                read_ops_per_sec,
                avg_read_latency,
                write_ops,
                write_ops_per_sec,
                avg_write_latency,
                read_errors,
                write_errors,
                memtable_size,
            ));
        }
        Ok::<(), std::io::Error>(())
    };

    let workload_future = async {
        for handle in handles {
            handle
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        }
        Ok::<(), std::io::Error>(())
    };

    tokio::try_join!(workload_future, reporting_task_future).unwrap();

    let total_read_ops = read_ops.load(Ordering::SeqCst);
    let total_write_ops = write_ops.load(Ordering::SeqCst);
    let total_read_errors = read_errors.load(Ordering::SeqCst);
    let total_write_errors = write_errors.load(Ordering::SeqCst);
    let elapsed_secs = start.elapsed().as_secs_f64();
    let read_ops_per_sec = (total_read_ops as f64) / elapsed_secs;
    let write_ops_per_sec = (total_write_ops as f64) / elapsed_secs;

    println!(
        "Performed {:.2} read operations per second",
        read_ops_per_sec
    );
    println!(
        "Performed {:.2} write operations per second",
        write_ops_per_sec
    );
    println!("Total reads: {}", total_read_ops);
    println!("Total read errors: {}", total_read_errors);
    println!("Total writes: {}", total_read_ops);
    println!("Total write errors: {}", total_write_errors);

    fs::remove_dir_all("/tmp/reprisedb_load_test").expect("Failed to remove directory");

    write_metrics_to_file(metrics)
        .await
        .expect("Failed to write metrics to file");
}

pub async fn write_metrics_to_file(
    metrics: Arc<Mutex<Vec<(f64, usize, f64, usize, usize, f64, usize, usize, usize, usize)>>>,
) -> std::io::Result<()> {
    let mut file = File::create("metrics.csv")?;
    let metrics = metrics.lock().await;

    // Write the header
    writeln!(file, "time,read_ops,read_ops_per_sec,avg_read_latency,write_ops,write_ops_per_sec,avg_write_latency,read_errors,write_errors,memtable_size")?;

    // Write the data
    for (
        elapsed_time,
        read_ops,
        read_ops_per_sec,
        avg_read_latency,
        write_ops,
        write_ops_per_sec,
        avg_write_latency,
        read_errors,
        write_errors,
        memtable_size,
    ) in metrics.iter()
    {
        writeln!(
            file,
            "{},{},{},{},{},{}, {}, {}, {}, {}",
            elapsed_time,
            read_ops,
            read_ops_per_sec,
            avg_read_latency,
            write_ops,
            write_ops_per_sec,
            avg_write_latency,
            read_errors,
            write_errors,
            memtable_size
        )?;
    }

    Ok(())
}
