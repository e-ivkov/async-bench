use rand::distributions::Standard;
use rand::Rng;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, Instant};

const TEST_FILE_NAME: &str = "test_file";
const N_THREADS: usize = 1000;
const N_ITERS: usize = 50;
const N_BYTES: usize = 10_000;

fn main() {
    println!("Generating file");
    let rng = rand::thread_rng();
    let mut data: Vec<u8> = rng.sample_iter(Standard).take(N_BYTES).collect();
    fs::write(TEST_FILE_NAME, &mut data).unwrap();
    println!("Generated file");

    ///////////////////////////////////////////////////////
    // simple_server
    //////////////////////////////////////////////////////
    thread::spawn(simple_server);
    thread::sleep(Duration::from_millis(1000));

    bench("simple server", "127.0.0.1:8080");
    {
        let mut stream = TcpStream::connect("127.0.0.1:8080").unwrap();
        stream.write_all(&[1]).unwrap();
    }
    thread::sleep(Duration::from_millis(1000));

    ///////////////////////////////////////////////////////
    // thread_pooled_server
    //////////////////////////////////////////////////////
    thread::spawn(thread_pooled_server);
    thread::sleep(Duration::from_millis(1000));

    bench("thread_pooled server", "127.0.0.1:8081");
    {
        let mut stream = TcpStream::connect("127.0.0.1:8080").unwrap();
        stream.write_all(&[1]).unwrap();
    }
    thread::sleep(Duration::from_millis(1000));

    ///////////////////////////////////////////////////////
    // tokio_server
    //////////////////////////////////////////////////////
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.spawn(tokio_server());
    thread::sleep(Duration::from_millis(1000));

    bench("tokio server", "127.0.0.1:8082");
    thread::sleep(Duration::from_millis(1000));

    fs::remove_file(TEST_FILE_NAME).unwrap();
}

fn bench(name: &str, addr: impl ToString) {
    let start = Instant::now();
    let mut join_handles = vec![];
    let addr = addr.to_string();
    for _ in 0..N_THREADS {
        let addr_clone = addr.clone();
        let jh = thread::spawn(move || {
            let mut response_times = Vec::new();
            for _ in 0..N_ITERS {
                let req_start = Instant::now();
                let mut stream = TcpStream::connect(&addr_clone).unwrap();
                stream.write_all(&[0]).unwrap();
                let mut buf = vec![];
                stream.read_to_end(&mut buf).unwrap();
                response_times.push(req_start.elapsed());
            }
            response_times
        });
        join_handles.push(jh);
    }
    let mut response_times = Vec::new();
    for jh in join_handles {
        response_times.append(&mut jh.join().unwrap())
    }

    let bytes_per_sec = (N_BYTES * N_THREADS * N_ITERS) as f64 / start.elapsed().as_secs_f64();
    let response_time = response_times
        .into_iter()
        .fold(0f64, |acc, d| acc + d.as_secs_f64())
        / (N_THREADS * N_ITERS) as f64;
    println!("{name} throughput: {} MB/s", bytes_per_sec / 1_000_000f64);
    println!("{name} response_time: {response_time} s");
}

fn thread_pooled_server() {
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    println!("Starting thread pooled server");
    let listener = TcpListener::bind("127.0.0.1:8081").unwrap();
    let should_break = Arc::new(AtomicBool::new(false));
    for conn in listener.incoming() {
        if should_break.load(Ordering::Relaxed) {
            break;
        }
        let mut conn = conn.unwrap();
        let should_break_move = should_break.clone();
        rayon::spawn(move || {
            let mut buf = vec![0];
            conn.read_exact(&mut buf).unwrap();
            if buf != [0] {
                should_break_move.store(true, Ordering::Relaxed);
            } else {
                let bytes = fs::read(TEST_FILE_NAME).unwrap();
                conn.write_all(&bytes).unwrap()
            }
        });
    }
    println!("Shutting down server")
}

fn simple_server() {
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    println!("Starting simple server");
    let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
    let should_break = Arc::new(AtomicBool::new(false));
    for conn in listener.incoming() {
        if should_break.load(Ordering::Relaxed) {
            break;
        }
        let mut conn = conn.unwrap();
        let should_break_move = should_break.clone();
        thread::spawn(move || {
            let mut buf = vec![0];
            conn.read_exact(&mut buf).unwrap();
            if buf != [0] {
                should_break_move.store(true, Ordering::Relaxed);
            } else {
                let bytes = fs::read(TEST_FILE_NAME).unwrap();
                conn.write_all(&bytes).unwrap()
            }
        });
    }
    println!("Shutting down server")
}

async fn tokio_server() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    println!("Starting tokio server");
    let listener = TcpListener::bind("127.0.0.1:8082").await.unwrap();
    let should_break = Arc::new(AtomicBool::new(false));
    while let Ok((mut conn, _)) = listener.accept().await {
        if should_break.load(Ordering::Relaxed) {
            break;
        }
        let should_break_move = should_break.clone();
        tokio::spawn(async move {
            let mut buf = vec![0];
            conn.read_exact(&mut buf).await.unwrap();
            if buf != [0] {
                should_break_move.store(true, Ordering::Relaxed);
            } else {
                let bytes = fs::read(TEST_FILE_NAME).unwrap();
                conn.write_all(&bytes).await.unwrap()
            }
        });
    }
    println!("Shutting down server")
}
