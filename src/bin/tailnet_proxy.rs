//! Tailnet-style replication proxy for fault injection.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use beads_rs::core::Limits;
use beads_rs::daemon::repl::frame::{FrameError, FrameReader, FrameWriter};

#[derive(Parser, Debug)]
#[command(name = "tailnet_proxy")]
struct Args {
    #[arg(long)]
    listen: String,
    #[arg(long)]
    upstream: String,
    #[arg(long, default_value = "tailnet")]
    profile: String,
    #[arg(long)]
    seed: Option<u64>,
    #[arg(long)]
    base_latency_ms: Option<u64>,
    #[arg(long)]
    jitter_ms: Option<u64>,
    #[arg(long)]
    loss_rate: Option<f64>,
    #[arg(long)]
    duplicate_rate: Option<f64>,
    #[arg(long)]
    reorder_rate: Option<f64>,
    #[arg(long)]
    blackhole_after_frames: Option<u64>,
    #[arg(long)]
    blackhole_after_bytes: Option<u64>,
    #[arg(long)]
    blackhole_for_ms: Option<u64>,
    #[arg(long)]
    reset_after_frames: Option<u64>,
    #[arg(long)]
    reset_after_bytes: Option<u64>,
    #[arg(long)]
    one_way_loss: Option<String>,
    #[arg(long)]
    max_frame_bytes: Option<usize>,
    #[arg(long, default_value = "off", value_enum)]
    trace_mode: TraceMode,
    #[arg(long)]
    trace_path: Option<PathBuf>,
    #[arg(long, default_value_t = 5_000)]
    trace_timeout_ms: u64,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum TraceMode {
    Off,
    Record,
    Replay,
}

impl TraceMode {
    fn as_str(self) -> &'static str {
        match self {
            TraceMode::Off => "off",
            TraceMode::Record => "record",
            TraceMode::Replay => "replay",
        }
    }
}

#[derive(Clone, Debug)]
struct TraceConfig {
    mode: TraceMode,
    path: PathBuf,
    timeout: Duration,
}

impl TraceConfig {
    fn from_args(args: &Args) -> Result<Option<Self>, String> {
        if args.trace_mode == TraceMode::Off {
            return Ok(None);
        }
        let path = args
            .trace_path
            .clone()
            .ok_or_else(|| "trace mode requires --trace-path".to_string())?;
        Ok(Some(Self {
            mode: args.trace_mode,
            path,
            timeout: Duration::from_millis(args.trace_timeout_ms.max(1)),
        }))
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum LossDirection {
    AtoB,
    BtoA,
}

impl LossDirection {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "a->b" | "a-to-b" | "a2b" => Some(LossDirection::AtoB),
            "b->a" | "b-to-a" | "b2a" => Some(LossDirection::BtoA),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Direction {
    AtoB,
    BtoA,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct Profile {
    base_latency_ms: u64,
    jitter_ms: u64,
    loss_rate: f64,
    duplicate_rate: f64,
    reorder_rate: f64,
    blackhole_after_frames: Option<u64>,
    blackhole_after_bytes: Option<u64>,
    blackhole_for_ms: Option<u64>,
    reset_after_frames: Option<u64>,
    reset_after_bytes: Option<u64>,
    one_way_loss: Option<LossDirection>,
}

impl Profile {
    fn tailnet() -> Self {
        Self {
            base_latency_ms: 20,
            jitter_ms: 30,
            loss_rate: 0.01,
            duplicate_rate: 0.002,
            reorder_rate: 0.01,
            blackhole_after_frames: None,
            blackhole_after_bytes: None,
            blackhole_for_ms: None,
            reset_after_frames: None,
            reset_after_bytes: None,
            one_way_loss: None,
        }
    }

    fn none() -> Self {
        Self {
            base_latency_ms: 0,
            jitter_ms: 0,
            loss_rate: 0.0,
            duplicate_rate: 0.0,
            reorder_rate: 0.0,
            blackhole_after_frames: None,
            blackhole_after_bytes: None,
            blackhole_for_ms: None,
            reset_after_frames: None,
            reset_after_bytes: None,
            one_way_loss: None,
        }
    }

    fn pathological() -> Self {
        Self {
            base_latency_ms: 15,
            jitter_ms: 40,
            loss_rate: 0.08,
            duplicate_rate: 0.0,
            reorder_rate: 0.02,
            blackhole_after_frames: Some(6),
            blackhole_after_bytes: None,
            blackhole_for_ms: Some(250),
            reset_after_frames: Some(24),
            reset_after_bytes: None,
            one_way_loss: None,
        }
    }

    fn for_direction(mut self, direction: Direction) -> Self {
        if let Some(loss_direction) = self.one_way_loss
            && !matches!(
                (direction, loss_direction),
                (Direction::AtoB, LossDirection::AtoB) | (Direction::BtoA, LossDirection::BtoA)
            )
        {
            self.loss_rate = 0.0;
        }
        self
    }

    fn sample_delay_ms(&self, rng: &mut StdRng) -> u64 {
        let jitter = if self.jitter_ms > 0 {
            rng.random_range(0..=self.jitter_ms)
        } else {
            0
        };
        self.base_latency_ms.saturating_add(jitter)
    }
}

#[derive(Clone)]
struct ScheduledFrame {
    deliver_at: Instant,
    payload: Vec<u8>,
}

const TRACE_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TraceRecord {
    Header { version: u32, max_frame_bytes: usize },
    Step {
        seq: u64,
        direction: Direction,
        payload_len: usize,
    },
}

#[derive(Debug)]
struct TraceStep {
    seq: u64,
    direction: Direction,
    payload_len: usize,
}

struct TraceWriter {
    writer: std::io::BufWriter<File>,
    next_seq: u64,
}

impl TraceWriter {
    fn new(path: &Path, max_frame_bytes: usize) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("trace dir create failed {:?}: {err}", parent))?;
        }
        let file =
            File::create(path).map_err(|err| format!("trace file create failed {path:?}: {err}"))?;
        let mut writer = std::io::BufWriter::new(file);
        let header = TraceRecord::Header {
            version: TRACE_VERSION,
            max_frame_bytes,
        };
        writeln!(
            writer,
            "{}",
            serde_json::to_string(&header).expect("serialize trace header")
        )
        .map_err(|err| format!("trace header write failed: {err}"))?;
        writer
            .flush()
            .map_err(|err| format!("trace header flush failed: {err}"))?;
        Ok(Self {
            writer,
            next_seq: 0,
        })
    }

    fn write_step(&mut self, direction: Direction, payload_len: usize) -> Result<(), String> {
        let record = TraceRecord::Step {
            seq: self.next_seq,
            direction,
            payload_len,
        };
        self.next_seq = self.next_seq.saturating_add(1);
        writeln!(
            self.writer,
            "{}",
            serde_json::to_string(&record).expect("serialize trace step")
        )
        .map_err(|err| format!("trace step write failed: {err}"))?;
        self.writer
            .flush()
            .map_err(|err| format!("trace step flush failed: {err}"))?;
        Ok(())
    }
}

struct TraceReader {
    max_frame_bytes: usize,
    steps: Vec<TraceStep>,
}

impl TraceReader {
    fn read(path: &Path) -> Result<Self, String> {
        let file =
            File::open(path).map_err(|err| format!("trace file open failed {path:?}: {err}"))?;
        let reader = BufReader::new(file);
        let mut max_frame_bytes = None;
        let mut steps = Vec::new();
        for line in reader.lines() {
            let line = line.map_err(|err| format!("trace read failed: {err}"))?;
            if line.trim().is_empty() {
                continue;
            }
            let record: TraceRecord =
                serde_json::from_str(&line).map_err(|err| format!("trace parse failed: {err}"))?;
            match record {
                TraceRecord::Header {
                    version,
                    max_frame_bytes: header_max,
                } => {
                    if version != TRACE_VERSION {
                        return Err(format!(
                            "trace version mismatch: expected {TRACE_VERSION} got {version}"
                        ));
                    }
                    max_frame_bytes = Some(header_max);
                }
                TraceRecord::Step {
                    seq,
                    direction,
                    payload_len,
                } => {
                    steps.push(TraceStep {
                        seq,
                        direction,
                        payload_len,
                    });
                }
            }
        }
        let max_frame_bytes =
            max_frame_bytes.ok_or_else(|| "trace missing header".to_string())?;
        Ok(Self {
            max_frame_bytes,
            steps,
        })
    }
}

impl Ord for ScheduledFrame {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap by delivery time.
        other
            .deliver_at
            .cmp(&self.deliver_at)
            .then_with(|| other.payload.len().cmp(&self.payload.len()))
    }
}

impl PartialOrd for ScheduledFrame {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ScheduledFrame {
    fn eq(&self, other: &Self) -> bool {
        self.deliver_at == other.deliver_at && self.payload.len() == other.payload.len()
    }
}

impl Eq for ScheduledFrame {}

fn main() {
    let args = Args::parse();
    let max_frame_bytes = args
        .max_frame_bytes
        .unwrap_or(Limits::default().max_frame_bytes);
    let seed = args.seed.unwrap_or(42);
    let mut profile = match args.profile.as_str() {
        "none" => Profile::none(),
        "pathology" | "pathological" => Profile::pathological(),
        _ => Profile::tailnet(),
    };
    let one_way_loss = args.one_way_loss.as_deref().and_then(LossDirection::parse);
    if args.one_way_loss.is_some() && one_way_loss.is_none() {
        eprintln!("invalid --one-way-loss value (use a->b or b->a)");
        std::process::exit(2);
    }
    profile.one_way_loss = one_way_loss;
    if let Some(value) = args.base_latency_ms {
        profile.base_latency_ms = value;
    }
    if let Some(value) = args.jitter_ms {
        profile.jitter_ms = value;
    }
    if let Some(value) = args.loss_rate {
        profile.loss_rate = value;
    }
    if let Some(value) = args.duplicate_rate {
        profile.duplicate_rate = value;
    }
    if let Some(value) = args.reorder_rate {
        profile.reorder_rate = value;
    }
    if let Some(value) = args.blackhole_after_frames {
        profile.blackhole_after_frames = Some(value);
    }
    if let Some(value) = args.blackhole_after_bytes {
        profile.blackhole_after_bytes = Some(value);
    }
    if let Some(value) = args.blackhole_for_ms {
        profile.blackhole_for_ms = Some(value);
    }
    if let Some(value) = args.reset_after_frames {
        profile.reset_after_frames = Some(value);
    }
    if let Some(value) = args.reset_after_bytes {
        profile.reset_after_bytes = Some(value);
    }

    let trace = TraceConfig::from_args(&args).unwrap_or_else(|err| {
        eprintln!("{err}");
        std::process::exit(2);
    });

    let listener = TcpListener::bind(&args.listen)
        .unwrap_or_else(|err| panic!("listen {} failed: {err}", args.listen));
    let mut connection_idx = 0u64;

    loop {
        let (client, _) = match listener.accept() {
            Ok(conn) => conn,
            Err(err) => {
                eprintln!("accept client failed: {err}");
                break;
            }
        };
        let upstream = match connect_with_retry(&args.upstream, Duration::from_secs(5)) {
            Ok(stream) => stream,
            Err(err) => {
                eprintln!("connect upstream {} failed: {err}", args.upstream);
                continue;
            }
        };

        let _ = client.set_nodelay(true);
        let _ = upstream.set_nodelay(true);

        let client_read = client.try_clone().expect("clone client");
        let client_write = client;
        let upstream_read = upstream.try_clone().expect("clone upstream");
        let upstream_write = upstream;

        if let Some(ref trace) = trace {
            run_trace_session(
                client_read,
                client_write,
                upstream_read,
                upstream_write,
                max_frame_bytes,
                trace,
            );
            break;
        }

        let conn_seed = seed ^ connection_idx.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        connection_idx = connection_idx.wrapping_add(1);

        let a_to_b = spawn_direction(
            "a->b",
            client_read,
            upstream_write,
            profile.for_direction(Direction::AtoB),
            conn_seed ^ 0xA5A5_A5A5_A5A5_A5A5,
            max_frame_bytes,
        );
        let b_to_a = spawn_direction(
            "b->a",
            upstream_read,
            client_write,
            profile.for_direction(Direction::BtoA),
            conn_seed ^ 0x5A5A_5A5A_5A5A_5A5A,
            max_frame_bytes,
        );

        let _ = a_to_b.join();
        let _ = b_to_a.join();
    }
}

fn connect_with_retry(addr: &str, timeout: Duration) -> Result<TcpStream, String> {
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(20);
    loop {
        match TcpStream::connect(addr) {
            Ok(stream) => return Ok(stream),
            Err(err) => {
                if Instant::now() >= deadline {
                    return Err(format!("{err}"));
                }
                thread::sleep(backoff);
                backoff = std::cmp::min(backoff.saturating_mul(2), Duration::from_millis(200));
            }
        }
    }
}

fn run_trace_session(
    client_read: TcpStream,
    client_write: TcpStream,
    upstream_read: TcpStream,
    upstream_write: TcpStream,
    max_frame_bytes: usize,
    trace: &TraceConfig,
) {
    match trace.mode {
        TraceMode::Record => run_trace_record(
            client_read,
            client_write,
            upstream_read,
            upstream_write,
            max_frame_bytes,
            trace,
        ),
        TraceMode::Replay => run_trace_replay(
            client_read,
            client_write,
            upstream_read,
            upstream_write,
            max_frame_bytes,
            trace,
        ),
        TraceMode::Off => {}
    }
}

enum IncomingFrame {
    Frame { direction: Direction, payload: Vec<u8> },
    Eof(Direction),
}

fn run_trace_record(
    client_read: TcpStream,
    client_write: TcpStream,
    upstream_read: TcpStream,
    upstream_write: TcpStream,
    max_frame_bytes: usize,
    trace: &TraceConfig,
) {
    let (tx, rx) = mpsc::channel::<IncomingFrame>();
    let a_tx = tx.clone();
    let b_tx = tx.clone();
    let a_reader = thread::spawn(move || trace_reader(Direction::AtoB, client_read, a_tx, max_frame_bytes));
    let b_reader =
        thread::spawn(move || trace_reader(Direction::BtoA, upstream_read, b_tx, max_frame_bytes));

    let mut writer_a = FrameWriter::new(client_write, max_frame_bytes);
    let mut writer_b = FrameWriter::new(upstream_write, max_frame_bytes);
    let mut trace_writer = match TraceWriter::new(&trace.path, max_frame_bytes) {
        Ok(writer) => writer,
        Err(err) => {
            eprintln!("{err}");
            return;
        }
    };

    let mut done = 0;
    while done < 2 {
        match rx.recv() {
            Ok(IncomingFrame::Frame { direction, payload }) => {
                if let Err(err) = forward_payload(direction, &payload, &mut writer_a, &mut writer_b)
                {
                    eprintln!("trace record write error: {}", frame_error_desc(&err));
                    break;
                }
                if let Err(err) = trace_writer.write_step(direction, payload.len()) {
                    eprintln!("{err}");
                    break;
                }
            }
            Ok(IncomingFrame::Eof(_)) => {
                done += 1;
            }
            Err(_) => break,
        }
    }

    let _ = a_reader.join();
    let _ = b_reader.join();
}

fn run_trace_replay(
    mut client_read: TcpStream,
    client_write: TcpStream,
    mut upstream_read: TcpStream,
    upstream_write: TcpStream,
    max_frame_bytes: usize,
    trace: &TraceConfig,
) {
    let trace_reader = match TraceReader::read(&trace.path) {
        Ok(reader) => reader,
        Err(err) => {
            eprintln!("{err}");
            return;
        }
    };
    if trace_reader.max_frame_bytes != max_frame_bytes {
        eprintln!(
            "trace max_frame_bytes mismatch: trace={} runtime={}",
            trace_reader.max_frame_bytes, max_frame_bytes
        );
    }
    let _ = client_read.set_read_timeout(Some(trace.timeout));
    let _ = upstream_read.set_read_timeout(Some(trace.timeout));

    let mut reader_a = FrameReader::new(client_read, max_frame_bytes);
    let mut reader_b = FrameReader::new(upstream_read, max_frame_bytes);
    let mut writer_a = FrameWriter::new(client_write, max_frame_bytes);
    let mut writer_b = FrameWriter::new(upstream_write, max_frame_bytes);

    for step in trace_reader.steps {
        let payload = match step.direction {
            Direction::AtoB => read_trace_frame(&mut reader_a, step.seq, step.direction),
            Direction::BtoA => read_trace_frame(&mut reader_b, step.seq, step.direction),
        };
        let Some(payload) = payload else {
            return;
        };
        if payload.len() != step.payload_len {
            eprintln!(
                "trace replay payload length mismatch at {}: expected {} got {}",
                step.seq,
                step.payload_len,
                payload.len()
            );
            return;
        }
        if let Err(err) = forward_payload(step.direction, &payload, &mut writer_a, &mut writer_b) {
            eprintln!("trace replay write error: {}", frame_error_desc(&err));
            return;
        }
    }
}

fn trace_reader(
    direction: Direction,
    reader: TcpStream,
    tx: mpsc::Sender<IncomingFrame>,
    max_frame_bytes: usize,
) {
    let mut frame_reader = FrameReader::new(reader, max_frame_bytes);
    loop {
        match frame_reader.read_next() {
            Ok(Some(payload)) => {
                if tx
                    .send(IncomingFrame::Frame { direction, payload })
                    .is_err()
                {
                    break;
                }
            }
            Ok(None) => {
                let _ = tx.send(IncomingFrame::Eof(direction));
                break;
            }
            Err(err) => {
                eprintln!("trace read error {direction:?}: {}", frame_error_desc(&err));
                let _ = tx.send(IncomingFrame::Eof(direction));
                break;
            }
        }
    }
}

fn read_trace_frame(
    reader: &mut FrameReader<TcpStream>,
    seq: u64,
    direction: Direction,
) -> Option<Vec<u8>> {
    match reader.read_next() {
        Ok(Some(payload)) => Some(payload),
        Ok(None) => {
            eprintln!("trace replay closed before step {seq} ({direction:?})");
            None
        }
        Err(err) => {
            eprintln!(
                "trace replay read error at {seq} ({direction:?}): {}",
                frame_error_desc(&err)
            );
            None
        }
    }
}

fn forward_payload(
    direction: Direction,
    payload: &[u8],
    writer_a: &mut FrameWriter<TcpStream>,
    writer_b: &mut FrameWriter<TcpStream>,
) -> Result<usize, FrameError> {
    match direction {
        Direction::AtoB => writer_b.write_frame(payload),
        Direction::BtoA => writer_a.write_frame(payload),
    }
}

fn spawn_direction(
    label: &'static str,
    reader: TcpStream,
    writer: TcpStream,
    profile: Profile,
    seed: u64,
    max_frame_bytes: usize,
) -> thread::JoinHandle<()> {
    let (tx, rx) = mpsc::channel::<ScheduledFrame>();
    let writer_handle = thread::spawn(move || run_scheduler(label, rx, writer, max_frame_bytes));
    let reader_handle =
        thread::spawn(move || run_reader(label, reader, tx, profile, seed, max_frame_bytes));
    thread::spawn(move || {
        let _ = reader_handle.join();
        let _ = writer_handle.join();
    })
}

fn run_reader(
    label: &'static str,
    reader: TcpStream,
    tx: mpsc::Sender<ScheduledFrame>,
    profile: Profile,
    seed: u64,
    max_frame_bytes: usize,
) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut frame_reader = FrameReader::new(reader, max_frame_bytes);
    let mut frames_seen: u64 = 0;
    let mut bytes_seen: u64 = 0;
    let mut blackhole_until: Option<Instant> = None;
    let mut blackhole_indefinite = false;
    let mut blackhole_after_frames = profile.blackhole_after_frames;
    let mut blackhole_after_bytes = profile.blackhole_after_bytes;
    loop {
        match frame_reader.read_next() {
            Ok(Some(payload)) => {
                frames_seen = frames_seen.saturating_add(1);
                bytes_seen = bytes_seen.saturating_add(payload.len() as u64);

                if profile
                    .reset_after_frames
                    .map(|threshold| frames_seen >= threshold)
                    .unwrap_or(false)
                    || profile
                        .reset_after_bytes
                        .map(|threshold| bytes_seen >= threshold)
                        .unwrap_or(false)
                {
                    eprintln!("proxy {label} injected reset");
                    break;
                }

                if let Some(until) = blackhole_until
                    && Instant::now() >= until
                {
                    blackhole_until = None;
                }

                let mut triggered_blackhole = false;
                if let Some(threshold) = blackhole_after_frames
                    && frames_seen >= threshold
                {
                    blackhole_after_frames = None;
                    triggered_blackhole = true;
                }
                if let Some(threshold) = blackhole_after_bytes
                    && bytes_seen >= threshold
                {
                    blackhole_after_bytes = None;
                    triggered_blackhole = true;
                }

                if triggered_blackhole {
                    if let Some(for_ms) = profile.blackhole_for_ms {
                        if for_ms == 0 {
                            blackhole_indefinite = true;
                        } else {
                            blackhole_until = Some(Instant::now() + Duration::from_millis(for_ms));
                        }
                    } else {
                        blackhole_indefinite = true;
                    }
                    eprintln!("proxy {label} entered blackhole");
                }

                if blackhole_indefinite || blackhole_until.is_some() {
                    continue;
                }

                if chance(&mut rng, profile.loss_rate) {
                    continue;
                }

                let mut delay_ms = profile.sample_delay_ms(&mut rng);
                if chance(&mut rng, profile.reorder_rate) {
                    delay_ms = 0;
                }
                let deliver_at = Instant::now() + Duration::from_millis(delay_ms);
                if tx
                    .send(ScheduledFrame {
                        deliver_at,
                        payload: payload.clone(),
                    })
                    .is_err()
                {
                    eprintln!("proxy {label} send failed (writer dropped)");
                    break;
                }

                if chance(&mut rng, profile.duplicate_rate) {
                    let dup_delay = delay_ms.saturating_add(1);
                    let deliver_at = Instant::now() + Duration::from_millis(dup_delay);
                    if tx
                        .send(ScheduledFrame {
                            deliver_at,
                            payload,
                        })
                        .is_err()
                    {
                        eprintln!("proxy {label} send failed (writer dropped)");
                        break;
                    }
                }
            }
            Ok(None) => break,
            Err(err) => {
                eprintln!("proxy {label} read error: {}", frame_error_desc(&err));
                break;
            }
        }
    }
}

fn run_scheduler(
    label: &'static str,
    rx: Receiver<ScheduledFrame>,
    writer: TcpStream,
    max_frame_bytes: usize,
) {
    let mut heap: BinaryHeap<ScheduledFrame> = BinaryHeap::new();
    let mut frame_writer = FrameWriter::new(writer, max_frame_bytes);
    loop {
        if let Some(next_due) = heap.peek().map(|frame| frame.deliver_at) {
            let now = Instant::now();
            if next_due <= now
                && let Some(frame) = heap.pop()
            {
                if let Err(err) = frame_writer.write_frame(&frame.payload) {
                    eprintln!("proxy {label} write error: {}", frame_error_desc(&err));
                    frame_writer.shutdown();
                    break;
                }
                continue;
            }

            let timeout = next_due.saturating_duration_since(now);
            match rx.recv_timeout(timeout) {
                Ok(frame) => heap.push(frame),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    if heap.is_empty() {
                        frame_writer.shutdown();
                        break;
                    }
                }
            }
        } else {
            match rx.recv() {
                Ok(frame) => heap.push(frame),
                Err(_) => {
                    frame_writer.shutdown();
                    break;
                }
            }
        }
    }
}

fn chance(rng: &mut StdRng, rate: f64) -> bool {
    if rate <= 0.0 {
        return false;
    }
    rng.random_range(0.0..1.0) < rate
}

fn frame_error_desc(err: &FrameError) -> String {
    match err {
        FrameError::Io(e) => format!("io {e}"),
        FrameError::FrameLengthInvalid { reason } => format!("length invalid: {reason}"),
        FrameError::FrameTooLarge {
            max_frame_bytes,
            got_bytes,
        } => format!("too large: {got_bytes} > {max_frame_bytes}"),
        FrameError::FrameCrcMismatch { expected, got } => {
            format!("crc mismatch expected {expected} got {got}")
        }
    }
}
