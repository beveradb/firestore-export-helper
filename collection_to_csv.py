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
from typing import Dict, Any, List, Set, Tuple
from collections import defaultdict, Counter

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

def analyze_document_types(documents: List[Dict[str, Any]], exclude_internal: bool = True) -> Dict[str, List[int]]:
    """Analyze and categorize documents by their field structure."""
    document_types = defaultdict(list)
    
    for i, doc in enumerate(documents):
        # Get all fields in this document
        flattened = flatten_dict(doc)
        
        if exclude_internal:
            fields = {k for k in flattened.keys() if not k.startswith('_')}
        else:
            fields = set(flattened.keys())
        
        # Create a signature based on the fields
        doc_signature = tuple(sorted(fields))
        document_types[doc_signature].append(i)
    
    return document_types

def get_field_coverage(documents: List[Dict[str, Any]], exclude_internal: bool = True) -> Dict[str, float]:
    """Get the coverage percentage for each field."""
    field_counts = Counter()
    total_docs = len(documents)
    
    for doc in documents:
        flattened = flatten_dict(doc)
        
        if exclude_internal:
            fields = {k for k in flattened.keys() if not k.startswith('_')}
        else:
            fields = set(flattened.keys())
        
        for field in fields:
            field_counts[field] += 1
    
    return {field: (count / total_docs) * 100 for field, count in field_counts.items()}

def export_collection_to_csv(
    json_file: str, 
    collection_name: str, 
    output_file: str = None,
    exclude_internal: bool = True,
    include_fields: List[str] = None,
    exclude_fields: List[str] = None,
    flatten_nested: bool = True,
    analyze_types: bool = False,
    filter_by_type: str = None,
    min_field_coverage: float = 0.0
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
        analyze_types: Whether to show document type analysis
        filter_by_type: Filter documents by type (e.g., "1" for type 1)
        min_field_coverage: Minimum field coverage percentage to include field
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
    
    print(f"Found {len(documents)} documents in '{collection_name}' collection...")
    
    # Analyze document types if requested
    if analyze_types:
        print("\n=== DOCUMENT TYPE ANALYSIS ===")
        document_types = analyze_document_types(documents, exclude_internal)
        field_coverage = get_field_coverage(documents, exclude_internal)
        
        print(f"Found {len(document_types)} different document types:")
        for i, (signature, doc_indices) in enumerate(document_types.items()):
            print(f"\nType {i+1}: {len(doc_indices)} documents ({len(doc_indices)/len(documents)*100:.1f}%)")
            print(f"Fields: {list(signature)}")
            print(f"Sample document indices: {doc_indices[:5]}{'...' if len(doc_indices) > 5 else ''}")
        
        print(f"\n=== FIELD COVERAGE ===")
        for field, coverage in sorted(field_coverage.items(), key=lambda x: x[1], reverse=True):
            print(f"{field}: {coverage:.1f}%")
        
        if not filter_by_type:
            print(f"\nTo export only documents of a specific type, use --filter-by-type option.")
            print(f"To filter out sparse fields, use --min-field-coverage option.")
            return
    
    # Filter documents by type if requested
    if filter_by_type:
        document_types = analyze_document_types(documents, exclude_internal)
        type_signatures = list(document_types.keys())
        
        try:
            type_index = int(filter_by_type) - 1
            if type_index < 0 or type_index >= len(type_signatures):
                print(f"Invalid type index. Available types: 1-{len(type_signatures)}")
                sys.exit(1)
            
            selected_signature = type_signatures[type_index]
            selected_indices = document_types[selected_signature]
            documents = [documents[i] for i in selected_indices]
            
            print(f"Filtered to {len(documents)} documents of type {filter_by_type}")
            
        except ValueError:
            print(f"Invalid type index: {filter_by_type}")
            sys.exit(1)
    
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
    
    # Apply field coverage filter
    if min_field_coverage > 0:
        field_coverage = get_field_coverage(processed_docs, exclude_internal)
        all_fields = {field for field in all_fields if field_coverage.get(field, 0) >= min_field_coverage}
        print(f"Filtered to {len(all_fields)} fields with coverage >= {min_field_coverage}%")
    
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
    
    # Determine output filename
    if not output_file:
        suffix = f"_type{filter_by_type}" if filter_by_type else ""
        output_file = f"{collection_name}{suffix}_export.csv"
    
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
  # Analyze document types in the collection
  python collection_to_csv.py firestore_export.json users --analyze-types
  
  # Export only user profile documents (type 1)
  python collection_to_csv.py firestore_export.json users --filter-by-type 1
  
  # Export only device session documents (type 2)
  python collection_to_csv.py firestore_export.json users --filter-by-type 2
  
  # Export only fields that appear in at least 50% of documents
  python collection_to_csv.py firestore_export.json users --min-field-coverage 50
  
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
    parser.add_argument('--analyze-types', action='store_true',
                       help='Analyze and show document type patterns')
    parser.add_argument('--filter-by-type', 
                       help='Filter documents by type (use --analyze-types first to see available types)')
    parser.add_argument('--min-field-coverage', type=float, default=0.0,
                       help='Minimum field coverage percentage to include field (0-100)')
    
    args = parser.parse_args()
    
    # Validate inputs
    if not Path(args.json_file).exists():
        print(f"JSON file not found: {args.json_file}")
        sys.exit(1)
    
    if args.min_field_coverage < 0 or args.min_field_coverage > 100:
        print("min-field-coverage must be between 0 and 100")
        sys.exit(1)
    
    export_collection_to_csv(
        json_file=args.json_file,
        collection_name=args.collection_name,
        output_file=args.output,
        exclude_internal=not args.include_internal,
        include_fields=args.include_fields,
        exclude_fields=args.exclude_fields,
        flatten_nested=not args.no_flatten,
        analyze_types=args.analyze_types,
        filter_by_type=args.filter_by_type,
        min_field_coverage=args.min_field_coverage
    )

if __name__ == "__main__":
    main() 