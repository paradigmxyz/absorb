
# Indexes

task that use index data
1. comparing collected_coverage vs available_coverage
2. splitting collection task into chunks
3. creating and parsing filenames

Use of index for each write_range type:
- overwrite_all:
    1. yes, need to compare collected vs available
    2. no, never need to split task into chunks
    3. chunks are named with index type if it has one, "all" otherwise
    - index_type is not required, without index type
- append_only:
    1. yes, need to compare collected vs available
    2. yes, always need to split task into chunks
    3. chunks are named according to chunk names
    - index_type is required


## Ranges
- two scales of ranges:
    - chunk ranges: describes size of a chunk
    - dataset ranges: describes entire size of a dataset
- Table.chunk_range indicates both
    1. what data_range is passed to collect_chunk()
    2. what will the file be named
- available range does not always divide evenly into chunks
    - e.g. if dataset continually updates or updates every day, and the chunk size is month
