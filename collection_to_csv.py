#!/usr/bin/env python3
"""
Collection to CSV Exporter

Export a specific collection from the converted Firestore JSON to CSV format.
"""

import json
import csv
import sys
import argparse
from pathlib import Path
from typing import Dict, Any, List, Set

def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    """Flatten a nested dictionary."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def get_all_fields(documents: List[Dict[str, Any]], exclude_internal: bool = True) -> Set[str]:
    """Get all unique fields from all documents in the collection."""
    all_fields = set()
    
    for doc in documents:
        # Flatten the document to get all possible fields
        flattened = flatten_dict(doc)
        
        if exclude_internal:
            # Exclude internal fields that start with _
            fields = {k for k in flattened.keys() if not k.startswith('_')}
        else:
            fields = set(flattened.keys())
        
        all_fields.update(fields)
    
    return all_fields

def export_collection_to_csv(
    json_file: str, 
    collection_name: str, 
    output_file: str = None,
    exclude_internal: bool = True,
    include_fields: List[str] = None,
    exclude_fields: List[str] = None,
    flatten_nested: bool = True
) -> None:
    """
    Export a collection to CSV format.
    
    Args:
        json_file: Path to the JSON file
        collection_name: Name of the collection to export
        output_file: Output CSV file name (optional)
        exclude_internal: Whether to exclude internal fields (_key, _document_id, etc.)
        include_fields: List of specific fields to include (optional)
        exclude_fields: List of fields to exclude (optional)
        flatten_nested: Whether to flatten nested objects
    """
    
    # Load the JSON data
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        sys.exit(1)
    
    # Get the collection
    collections = data.get('collections', {})
    if collection_name not in collections:
        print(f"Collection '{collection_name}' not found.")
        print(f"Available collections: {list(collections.keys())}")
        sys.exit(1)
    
    documents = collections[collection_name]
    if not documents:
        print(f"Collection '{collection_name}' is empty.")
        sys.exit(1)
    
    print(f"Exporting {len(documents)} documents from '{collection_name}' collection...")
    
    # Determine output filename
    if not output_file:
        output_file = f"{collection_name}_export.csv"
    
    # Process documents
    processed_docs = []
    for doc in documents:
        if flatten_nested:
            processed_doc = flatten_dict(doc)
        else:
            # Convert nested objects to JSON strings
            processed_doc = {}
            for k, v in doc.items():
                if isinstance(v, (dict, list)):
                    processed_doc[k] = json.dumps(v, default=str)
                else:
                    processed_doc[k] = v
        
        processed_docs.append(processed_doc)
    
    # Determine fields to include
    all_fields = get_all_fields(processed_docs, exclude_internal)
    
    if include_fields:
        # Only include specified fields
        fields_to_export = set(include_fields) & all_fields
        missing_fields = set(include_fields) - all_fields
        if missing_fields:
            print(f"Warning: These fields were not found: {missing_fields}")
    else:
        fields_to_export = all_fields
    
    if exclude_fields:
        # Exclude specified fields
        fields_to_export = fields_to_export - set(exclude_fields)
    
    # Sort fields for consistent output
    fields_to_export = sorted(fields_to_export)
    
    if not fields_to_export:
        print("No fields to export after applying filters.")
        sys.exit(1)
    
    print(f"Exporting {len(fields_to_export)} fields: {fields_to_export}")
    
    # Write to CSV
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields_to_export, restval='')
            writer.writeheader()
            
            for doc in processed_docs:
                # Only write fields that we want to export
                row = {field: doc.get(field, '') for field in fields_to_export}
                writer.writerow(row)
        
        print(f"Successfully exported to {output_file}")
        
    except Exception as e:
        print(f"Error writing CSV file: {e}")
        sys.exit(1)

def main():
    """Main function with argument parsing."""
    parser = argparse.ArgumentParser(
        description='Export a Firestore collection to CSV format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export users collection with default settings
  python collection_to_csv.py firestore_export.json users
  
  # Export to specific file, including internal fields
  python collection_to_csv.py firestore_export.json users --output users_full.csv --include-internal
  
  # Export only specific fields
  python collection_to_csv.py firestore_export.json users --include-fields email display_name created_time
  
  # Export excluding certain fields
  python collection_to_csv.py firestore_export.json users --exclude-fields bio phone_number
  
  # Export without flattening nested objects
  python collection_to_csv.py firestore_export.json users --no-flatten
        """
    )
    
    parser.add_argument('json_file', help='Path to the JSON file')
    parser.add_argument('collection_name', help='Name of the collection to export')
    parser.add_argument('--output', '-o', help='Output CSV file name')
    parser.add_argument('--include-internal', action='store_true', 
                       help='Include internal fields (_key, _document_id, etc.)')
    parser.add_argument('--include-fields', nargs='+', 
                       help='Only include these specific fields')
    parser.add_argument('--exclude-fields', nargs='+', 
                       help='Exclude these specific fields')
    parser.add_argument('--no-flatten', action='store_true', 
                       help='Do not flatten nested objects (convert to JSON strings instead)')
    
    args = parser.parse_args()
    
    # Validate inputs
    if not Path(args.json_file).exists():
        print(f"JSON file not found: {args.json_file}")
        sys.exit(1)
    
    export_collection_to_csv(
        json_file=args.json_file,
        collection_name=args.collection_name,
        output_file=args.output,
        exclude_internal=not args.include_internal,
        include_fields=args.include_fields,
        exclude_fields=args.exclude_fields,
        flatten_nested=not args.no_flatten
    )

if __name__ == "__main__":
    main() 