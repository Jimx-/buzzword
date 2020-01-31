# Buzzword

[![Build Status](https://ci2.jimx.site:8080/buildStatus/icon?job=buzzword)](https://ci2.jimx.site:8080/job/buzzword/)

Buzzword is lock-free Bw-Tree ([paper](https://www.microsoft.com/en-us/research/publication/the-bw-tree-a-b-tree-for-new-hardware/)) implementation that supports fast insertions/queries of billions of key-value pairs.

## Example

```rust
let tree = BwTree::new(1024 * 1024 /* mapping table size */);

tree.insert(&1, &quot;alice&quot;);
tree.insert(&2, &quot;bob&quot;);

let vals = tree.get_values(&1);
assert_eq!(vals.len(), 1);
assert_eq!(vals[0], &quot;alice&quot;);

for (k, v) in &tree {
 println!(&quot;{} -&gt; {}&quot;, k, v);
}

assert!(tree.delete(&1, &&quot;alice&quot;));
```

## Performance

On Intel(R) Xeon(R) Gold 6230 (80 threads):

| Workload                           | Avg. Throughput (MOp/s) |
| ---------------------------------- | ----------------------- |
| 1B (u32, u32) concurrent insert    | 11.53                   |
| 1B (u32, u32) concurrent seq. scan | 78.52                   |
| 1B (u32, u32) concurrent delete    | 13.49                   |
