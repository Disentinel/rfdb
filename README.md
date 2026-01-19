# RFDB (ReginaFlowDB)

High-performance disk-backed graph engine for semantic code analysis.

## Features

- **Columnar storage**: nodes.bin, edges.bin, strings.bin
- **Deterministic IDs**: BLAKE3 hash from (type|name|scope|path)
- **Zero-copy access**: memmap2 without copying to RAM
- **Delta log**: In-memory buffer for fast writes
- **Version-aware**: Support for main / __local versions for incremental analysis
- **BFS/DFS traversal**: Fast graph traversals

## Installation

### As npm package

```bash
npm install @grafema/rfdb
```

### From source

```bash
git clone https://github.com/grafema/rfdb.git
cd rfdb
cargo build --release
```

## Usage

### Basic example (Rust)

```rust
use rfdb::{GraphEngine, NodeRecord, EdgeRecord};

let mut engine = GraphEngine::create("./graph.rfdb")?;

// Add nodes
engine.add_nodes(vec![
    NodeRecord {
        id: 123456789,
        kind: "FUNCTION".to_string(),
        file_id: 1,
        name_offset: 10,
        version: "main".into(),
        exported: true,
        replaces: None,
        deleted: false,
        name: "myFunction".to_string(),
        metadata: None,
    }
]);

// BFS traversal
let endpoints = engine.bfs(&[123456789], 10, &["CALLS"])?; // depth=10, edge_type=CALLS
println!("Found {} endpoints", endpoints.len());
```

### Node.js usage

```javascript
const { GraphEngine } = require('@grafema/rfdb');

const engine = GraphEngine.create('./graph.rfdb');

// Add nodes
engine.addNodes([{
    id: 123456789n,
    kind: 'FUNCTION',
    fileId: 1,
    name: 'myFunction',
    version: 'main',
    exported: true,
}]);

// BFS traversal
const endpoints = engine.bfs([123456789n], 10, ['CALLS']);
console.log(`Found ${endpoints.length} endpoints`);
```

### Run examples

```bash
cargo run --example basic_usage
```

## Benchmarks

Run benchmarks:

```bash
# Internal benchmarks (Rust only)
cargo bench --bench graph_operations

# Comparative benchmarks (Rust vs Neo4j)
# NOTE: Requires running Neo4j on localhost:7687
cargo bench --bench neo4j_comparison
```

### Expected results

| Operation | Neo4j | RFDB | Speedup |
|-----------|-------|------|---------|
| Batch write (1000 nodes) | ~200ms | ~10ms | **20x** |
| Find by type (100k nodes) | ~500ms | ~50ms | **10x** |
| BFS (depth=10) | ~100ms | ~20ms | **5x** |
| Full scan (100k nodes) | ~500ms | ~50ms | **10x** |

## Project structure

```
rfdb/
├── src/
│   ├── graph/          # Graph API and implementation
│   │   ├── engine.rs   # GraphEngine implementation
│   │   ├── traversal.rs # BFS/DFS algorithms
│   │   └── id_gen.rs   # BLAKE3 ID generation
│   ├── storage/        # Columnar storage
│   │   ├── segment.rs  # Immutable mmap segments
│   │   ├── delta.rs    # Delta log for updates
│   │   └── string_table.rs # String interning
│   ├── index/          # Secondary indexes (sled)
│   └── error.rs        # Error types
├── benches/            # Criterion benchmarks
├── examples/           # Usage examples
└── Cargo.toml
```

## Data format

### nodes.bin

Columnar format:
```
[Header: 32 bytes]
[ids: u128 × N]
[kinds: u16 × N]
[file_ids: u32 × N]
[name_offsets: u32 × N]
[deleted: u8 × N]
```

### edges.bin

```
[Header: 32 bytes]
[src: u128 × N]
[dst: u128 × N]
[etypes: u16 × N]
[deleted: u8 × N]
```

### strings.bin

```
[data_len: u64]
[data: u8 × data_len]
[offsets_count: u64]
[offsets: u32 × offsets_count]
```

## Comparison with Neo4j

### Supported

- Batch operations (nodes/edges)
- Attribute filtering
- BFS/DFS traversal
- Variable-length paths
- Soft deletes (tombstones)
- Version-aware queries

### Requires manual implementation

- Aggregations (COUNT, DISTINCT, etc.)
- Full-text search
- Complex path patterns

### Not supported

- ACID transactions
- Cypher query language
- Automatic indexes
- Database constraints
- Remote multi-user access

## Performance tips

1. **Batch operations**: Always use `add_nodes()` / `add_edges()` with vectors
2. **Flush frequency**: Call `flush()` every 10k operations
3. **Compaction**: Run `compact()` after large changes
4. **Edge types filter**: Specify concrete edge types in `bfs()`

## Roadmap

- [ ] Real mmap segments (currently in-memory HashMap)
- [ ] Background compaction thread
- [ ] Write-ahead log for durability
- [ ] Parallel BFS via rayon
- [ ] JSON-RPC server for MCP integration
- [ ] SIMD optimizations for scanning
- [ ] Real Neo4j connector for migration

## License

Apache-2.0
