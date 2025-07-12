# Firestore LevelDB to JSON Converter

This toolkit converts Firestore database exports from LevelDB format to JSON format, making it easier to work with and analyze the data.

## Files Created

1. **firestore_to_json.py** - Main conversion script
2. **firestore_viewer.py** - Interactive viewer for exploring the converted data
3. **collection_to_csv.py** - Export specific collections to CSV format
4. **requirements.txt** - Python dependencies

## Prerequisites

Install the required package:
```bash
pip install leveldb-export
```

## Usage

### 1. Convert LevelDB Export to JSON

```bash
python firestore_to_json.py <export_directory> [output_file]
```

**Example:**
```bash
python firestore_to_json.py ./backups/all_namespaces/all_kinds firestore_export.json
```

**Parameters:**
- `export_directory`: Path to the directory containing the LevelDB export files (output-0, output-1, etc.)
- `output_file`: Optional. Name of the output JSON file (default: firestore_export.json)

### 2. Explore the Converted Data

```bash
python firestore_viewer.py <json_file>
```

**Example:**
```bash
python firestore_viewer.py firestore_export_fixed.json
```

### 3. Export Collection to CSV

```bash
python collection_to_csv.py <json_file> <collection_name> [options]
```

**Examples:**
```bash
# Export users collection with default settings
python collection_to_csv.py firestore_export.json users

# Export only specific fields
python collection_to_csv.py firestore_export.json users --include-fields email display_name created_time

# Export to specific file, excluding certain fields
python collection_to_csv.py firestore_export.json users --output users_clean.csv --exclude-fields bio phone_number

# Export including internal fields (_key, _document_id, etc.)
python collection_to_csv.py firestore_export.json users --include-internal
```

## Interactive Viewer Commands

Once in the interactive viewer, you can use these commands:

- `summary` - Show data summary
- `list` - List all collections
- `show <collection>` - Show collection details
- `doc <collection> <index>` - Show specific document
- `search <collection> <field> <value>` - Search documents
- `quit` - Exit

**Example session:**
```
> summary
> list
> show karaoke_songs
> doc karaoke_songs 0
> search karaoke_songs artist "Beatles"
> quit
```

## Your Data Summary

Based on your conversion, your Firestore database contains:

- **Total documents**: 31,504
- **Total collections**: 4
- **Export files processed**: 242

### Collections:
- `ff_push_notifications`: 7 documents
- `karaoke_brands`: 97 documents  
- `karaoke_songs`: 31,057 documents
- `users`: 343 documents

## Output Structure

The JSON output has the following structure:

```json
{
  "metadata": {
    "total_documents": 31504,
    "total_collections": 4,
    "export_files_processed": 242,
    "export_metadata": "..."
  },
  "collections": {
    "collection_name": [
      {
        "field1": "value1",
        "field2": "value2",
        "_document_id": "document_id",
        "_key": {
          "id": null,
          "name": "document_id",
          "namespace": "",
          "app": "s~projectbread-karaokay",
          "path": "collection_name/document_id"
        }
      }
    ]
  }
}
```

## Features

- **Automatic collection detection**: Documents are automatically organized by collection based on their Firestore path
- **Document ID extraction**: Each document includes its Firestore document ID as `_document_id`
- **Metadata preservation**: Original Firestore metadata is preserved
- **Error handling**: Graceful handling of parsing errors
- **Progress tracking**: Real-time progress during conversion
- **Interactive exploration**: Easy-to-use viewer for exploring the data
- **CSV export**: Export specific collections to CSV format with field filtering options
- **Flexible field selection**: Include/exclude specific fields, handle nested objects

## Sample Usage for Your Data

```bash
# Convert your export
python firestore_to_json.py ./backups/all_namespaces/all_kinds my_karaoke_data.json

# Explore the karaoke songs
python firestore_viewer.py my_karaoke_data.json
> show karaoke_songs
> search karaoke_songs artist "Beatles"
> doc karaoke_songs 0

# Export users to CSV
python collection_to_csv.py my_karaoke_data.json users

# Export karaoke songs with specific fields
python collection_to_csv.py my_karaoke_data.json karaoke_songs --include-fields artist title youtube_link brand
```

## Notes

- The conversion preserves all original data types including datetime objects (converted to ISO strings in JSON)
- Large exports may take some time to process
- The viewer loads the entire JSON file into memory, so very large exports may require more RAM
- Original LevelDB files are not modified during conversion

## Troubleshooting

If you encounter issues:

1. **Unicode errors**: The script handles different encodings automatically
2. **Memory issues**: For very large exports, consider processing in smaller batches
3. **Missing documents**: Check that all output-* files are present in the export directory
4. **Performance**: The conversion is optimized but may take time for large datasets

## Next Steps

With your data now in JSON format, you can:
- Import into other databases
- Analyze with data science tools
- Create backups in a readable format
- Build custom applications using the data
- Perform advanced queries and analysis 