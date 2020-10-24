# HashArrayCompressedMAP or hacmap

Memory efficient immutable collection for key/value pair, where key is ID or hash.

Can be used as in memory catalog to data on NVMe storages for example.

Header-Only Library!

Little example, assume you have 2 million of key/value pairs, lets calculate memory usage:

**  array
**  std::unordered_map
**  eh_map
**  eh_compressed_map

Time of key locate:
